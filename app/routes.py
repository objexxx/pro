import pandas as pd
import sqlite3
import random
import os
import re
import csv
import io
import math
import threading
import time
from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, send_from_directory, jsonify, redirect, url_for, current_app, Response
from flask_login import login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from .models import User, get_db
from .extensions import limiter
from .services.parser import OrderParser
from .services.label_engine import LabelEngine
from .services.amazon_confirmer import run_confirmation, parse_cookies_and_csrf, validate_session

main_bp = Blueprint('main', __name__)

# --- STRICT CONFIGURATION ---
STRICT_HEADERS = [
    'No', 'FromName', 'PhoneFrom', 'Street1From', 'CompanyFrom', 'Street2From', 
    'CityFrom', 'StateFrom', 'PostalCodeFrom', 'ToName', 'PhoneTo', 'Street1To', 
    'Company2', 'Street2To', 'CityTo', 'StateTo', 'ZipTo', 'Weight', 'Length', 
    'Width', 'Height', 'Description', 'Ref01', 'Ref02', 'Contains Hazard', 'Shipment Date'
]

# --- CONCURRENCY LOCKS ---
ACTIVE_CONFIRMATIONS = set() 

# --- HELPER FUNCTIONS ---
def normalize_dataframe(df):
    df.columns = [str(c).strip() for c in df.columns]
    current_headers = list(df.columns)
    if current_headers != STRICT_HEADERS:
        return None, "Format Error: Please use the updated 'Download Format' template."

    order_col = 'No'
    if order_col in df.columns:
        if df[order_col].isnull().any() or (df[order_col] == '').any():
            return None, f"Row Error: Missing Order Number in '{order_col}' column."

    incomplete_rows = df[df['ToName'].isnull() | (df['ToName'].astype(str).str.strip() == '') | 
                          df['Street1To'].isnull() | (df['Street1To'].astype(str).str.strip() == '')]
    if not incomplete_rows.empty:
        first_error_idx = incomplete_rows.index[0] + 2
        return None, f"Row {first_error_idx} Error: Missing required Recipient Information."
    return df, None

def get_system_config():
    conn = get_db(); c = conn.cursor()
    try:
        c.execute("SELECT key, value FROM system_config")
        rows = dict(c.fetchall())
    except: rows = {}
    conn.close()
    return rows

def get_price(user_id, label_type, version, default_price):
    try:
        conn = get_db(); c = conn.cursor()
        c.execute("SELECT price FROM user_pricing WHERE user_id = ? AND label_type = ? AND version = ?", (user_id, label_type, version))
        row = c.fetchone(); conn.close()
        if row: return float(row[0])
    except: pass
    return default_price

def to_est(date_str):
    try:
        if not date_str: return ""
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        est = dt - timedelta(hours=5)
        return est.strftime("%Y-%m-%d %H:%M:%S")
    except: return date_str

# --- AUTH ROUTES ---
@main_bp.route('/')
def index():
    if current_user.is_authenticated:
        if current_user.is_admin:
            return redirect(url_for('admin.dashboard'))
        return redirect(url_for('main.purchase'))
    return redirect(url_for('main.login'))

@main_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user_data = User.get_by_username(username)
        
        if user_data and check_password_hash(user_data[3], password):
            user = User.get(user_data[0]) 
            login_user(user)
            
            # --- LOG IP ADDRESS ---
            try:
                conn = get_db(); c = conn.cursor()
                ip = request.headers.get('X-Forwarded-For', request.remote_addr)
                ua = request.headers.get('User-Agent', '')[:200]
                c.execute("INSERT INTO login_history (user_id, ip_address, user_agent, created_at) VALUES (?, ?, ?, ?)",
                          (user.id, ip, ua, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
                conn.commit(); conn.close()
            except: pass
            
            # --- ADMIN REDIRECT ---
            if user.is_admin:
                return redirect(url_for('admin.dashboard'))
            
            return redirect(url_for('main.purchase'))
            
        return render_template('login.html', error="INVALID CREDENTIALS")
    return render_template('login.html')

@main_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        hashed = generate_password_hash(password)
        if User.create(username, email, hashed): return redirect(url_for('main.login'))
        else: return render_template('login.html', mode="register", error="USERNAME TAKEN")
    return render_template('login.html', mode="register")

@main_bp.route('/logout')
@login_required
def logout(): logout_user(); return redirect(url_for('main.login'))

# --- DASHBOARD ROUTES ---
@main_bp.route('/dashboard')
@login_required
def dashboard_root(): return redirect(url_for('main.purchase'))

@main_bp.route('/purchase')
@login_required
def purchase(): return render_template('dashboard.html', user=current_user, active_tab='purchase')

@main_bp.route('/history')
@login_required
def history(): return render_template('dashboard.html', user=current_user, active_tab='history')

@main_bp.route('/automation')
@login_required
def automation(): 
    sys_config = get_system_config()
    monthly_left = int(sys_config.get('slots_monthly_total', 50)) - int(sys_config.get('slots_monthly_used', 0))
    lifetime_left = int(sys_config.get('slots_lifetime_total', 10)) - int(sys_config.get('slots_lifetime_used', 0))
    return render_template('dashboard.html', user=current_user, active_tab='automation', monthly_left=monthly_left, lifetime_left=lifetime_left, system_status="OPERATIONAL")

@main_bp.route('/stats')
@login_required
def stats(): return render_template('dashboard.html', user=current_user, active_tab='stats')

@main_bp.route('/deposit')
@login_required
def deposit(): return render_template('dashboard.html', user=current_user, active_tab='deposit')

@main_bp.route('/settings')
@login_required
def settings(): return render_template('dashboard.html', user=current_user, active_tab='settings')

@main_bp.route('/addresses')
@login_required
def addresses(): return render_template('dashboard.html', user=current_user, active_tab='addresses')

# --- API ENDPOINTS ---
@main_bp.route('/api/user')
@login_required
def api_user():
    return jsonify({"username": current_user.username, "balance": current_user.balance, "price_per_label": current_user.price_per_label})

@main_bp.route('/api/settings/defaults', methods=['POST'])
@login_required
def save_defaults():
    data = request.json
    l_type = data.get('label_type', 'priority')
    ver = data.get('version', '95055')
    tmpl = data.get('template', 'pitney_v2')
    current_user.update_defaults(l_type, ver, tmpl)
    return jsonify({"status": "success", "message": "DEFAULTS SAVED"})

@main_bp.route('/api/batches')
@login_required
def api_batches():
    page = int(request.args.get('page', 1))
    limit = 10
    offset = (page - 1) * limit
    search = request.args.get('search', '').strip()
    sort_by = request.args.get('sort', 'recent')
    
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT batch_id FROM batches WHERE status = 'QUEUED' ORDER BY created_at ASC")
    queue_list = [row[0] for row in c.fetchall()]

    query = "SELECT * FROM batches WHERE user_id = ?"
    params = [current_user.id]
    
    if search:
        query += " AND (batch_id LIKE ? OR filename LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
    
    query += " ORDER BY"
    if sort_by == 'oldest': query += " created_at ASC"
    elif sort_by == 'high': query += " count DESC"
    else: query += " created_at DESC"
    
    c.execute(query.replace("SELECT *", "SELECT COUNT(*)"), params)
    total_items = c.fetchone()[0]
    total_pages = math.ceil(total_items / limit)
    
    query += " LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()
    
    data = []
    now = datetime.utcnow()

    for r in rows:
        b_id = r[0]
        full_filename = r[2]
        status = r[5]
        utc_date = r[9]
        clean_name = full_filename.split('_', 1)[1] if '_' in full_filename else full_filename
        est_date = to_est(utc_date)
        queue_pos = queue_list.index(b_id) + 1 if status == 'QUEUED' and b_id in queue_list else -1

        is_expired = False
        try:
            created_dt = datetime.strptime(utc_date, "%Y-%m-%d %H:%M:%S")
            age = now - created_dt
            if age.days >= 7: is_expired = True
        except: pass

        data.append({
            "batch_id": b_id, 
            "batch_name": clean_name,
            "count": r[3], 
            "success_count": r[4], 
            "status": status, 
            "date": est_date,
            "queue_pos": queue_pos,
            "is_expired": is_expired
        })
    
    return jsonify({"data": data, "pagination": {"current_page": page, "total_pages": total_pages}})

@main_bp.route('/api/stats')
@login_required
def api_stats():
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT SUM(success_count) FROM batches WHERE user_id = ?", (current_user.id,))
    total = c.fetchone()[0] or 0
    c.execute("SELECT COUNT(*) FROM batches WHERE user_id = ?", (current_user.id,))
    batches = c.fetchone()[0] or 0
    conn.close()
    return jsonify({"total_labels": total, "total_batches": batches})

@main_bp.route('/process', methods=['POST'])
@login_required
@limiter.limit("10 per minute") 
def process():
    if 'file' not in request.files: return jsonify({"error": "No file uploaded"}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({"error": "No selected file"}), 400

    template_choice = request.form.get('template_choice') or current_user.default_template
    version_choice = request.form.get('tracking_version') or current_user.default_version
    label_type = request.form.get('label_type') or current_user.default_label_type

    try:
        file.stream.seek(0)
        df = pd.read_csv(file, encoding='utf-8-sig', on_bad_lines='skip', dtype=str)
    except Exception as e:
        return jsonify({"error": f"CSV Read Failed: {str(e)}"}), 400

    df, error_msg = normalize_dataframe(df)
    if error_msg: return jsonify({"error": error_msg}), 400
    
    price_per_label = get_price(current_user.id, label_type, version_choice, current_user.price_per_label)
    cost = len(df) * price_per_label
    
    if not current_user.update_balance(-cost): 
        return jsonify({"error": "INSUFFICIENT FUNDS"}), 402
    
    try:
        batch_id = str(random.randint(100000, 999999))
        original_name = secure_filename(file.filename)
        if not original_name: original_name = "upload.csv"
        filename = f"{batch_id}_{original_name}"
        
        save_path = os.path.join(current_app.config['DATA_FOLDER'], 'uploads', filename)
        df.to_csv(save_path, index=False)
        
        conn = get_db(); c = conn.cursor()
        utc_now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        
        c.execute("INSERT INTO batches (batch_id, user_id, filename, count, success_count, status, template, version, label_type, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                  (batch_id, current_user.id, filename, len(df), 0, 'QUEUED', template_choice, version_choice, label_type, utc_now))
        conn.commit()
        conn.close()
        
        return jsonify({"status": "success", "batch_id": batch_id})

    except Exception as e:
        current_user.update_balance(cost)
        return jsonify({"error": f"System Error: {str(e)}"}), 500

@main_bp.route('/verify-csv', methods=['POST'])
@login_required
def verify_csv():
    if 'file' not in request.files: return jsonify({"error": "No file uploaded"}), 400
    file = request.files['file']
    try:
        file.stream.seek(0)
        df = pd.read_csv(file, encoding='utf-8-sig', on_bad_lines='skip', dtype=str)
    except Exception as e:
        return jsonify({"error": f"CSV Read Failed: {str(e)}"}), 400

    df, error_msg = normalize_dataframe(df)
    if error_msg: return jsonify({"error": error_msg}), 400

    return jsonify({"count": len(df), "cost": len(df) * current_user.price_per_label})

@main_bp.route('/api/automation/purchase', methods=['POST'])
@login_required
def automation_purchase():
    data = request.json
    plan = data.get('plan', 'monthly')
    if current_user.is_subscribed: return jsonify({"error": "ACTIVE SUBSCRIPTION FOUND"}), 400
    
    cost = 499.00 if plan == 'lifetime' else 29.99
    days = 36500 if plan == 'lifetime' else 30
    key_used = 'slots_lifetime_used' if plan == 'lifetime' else 'slots_monthly_used'
    key_total = 'slots_lifetime_total' if plan == 'lifetime' else 'slots_monthly_total'
    
    conn = get_db(); c = conn.cursor()
    try:
        c.execute("SELECT value FROM system_config WHERE key = ?", (key_used,))
        row = c.fetchone()
        used = int(row[0]) if row else 0
        c.execute("SELECT value FROM system_config WHERE key = ?", (key_total,))
        row = c.fetchone()
        total = int(row[0]) if row else 50
        if used >= total:
            conn.close()
            return jsonify({"error": f"SOLD OUT: {plan.upper()} KEYS UNAVAILABLE"}), 400
    except: pass

    if current_user.update_balance(-cost):
        current_user.activate_subscription(days, plan != 'lifetime')
        try:
            c.execute("UPDATE system_config SET value = ? WHERE key = ?", (str(used + 1), key_used))
            conn.commit()
        except: pass
        conn.close()
        return jsonify({"status": "success", "message": f"{plan.upper()} LICENSE ACTIVATED"})
    conn.close()
    return jsonify({"error": "INSUFFICIENT FUNDS"}), 402

@main_bp.route('/api/automation/save', methods=['POST'])
@login_required
def automation_save():
    if not current_user.is_subscribed: return jsonify({"error": "LICENSE REQUIRED"}), 403
    cookies = request.form.get('cookies', '')
    csrf = request.form.get('csrf', '').strip()
    inventory = request.form.get('inventory', '')
    current_user.update_settings(cookies, csrf, "", inventory, False)
    return jsonify({"status": "success", "message": "SETTINGS SAVED"})

@main_bp.route('/api/automation/format', methods=['POST'])
@login_required
@limiter.limit("50 per minute")
def automation_format():
    if not current_user.is_subscribed: return jsonify({"error": "LICENSE REQUIRED"}), 403
    file = request.files.get('file')
    if not file: return jsonify({"error": "NO FILE UPLOADED"}), 400
    address_id = request.form.get('address_id'); sender_address = None
    if address_id:
        conn = get_db(); c = conn.cursor()
        c.execute("SELECT name, company, street1, street2, city, state, zip, phone FROM sender_addresses WHERE id = ? AND user_id = ?", (address_id, current_user.id))
        row = c.fetchone(); conn.close()
        if row: sender_address = {'name': row[0], 'company': row[1], 'street1': row[2], 'street2': row[3], 'city': row[4], 'state': row[5], 'zip': row[6], 'phone': row[7]}
    content = file.read()
    zip_bytes, error = OrderParser.parse_to_zip(content, current_user.inventory_json, sender_address)
    if error: return jsonify({"error": error}), 400
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    return Response(zip_bytes, mimetype="application/zip", headers={"Content-disposition": f"attachment; filename=Parsed_Orders_{timestamp}.zip"})

@main_bp.route('/api/automation/confirm', methods=['POST'])
@login_required
@limiter.limit("60 per minute")
def automation_confirm():
    if not current_user.is_subscribed: return jsonify({"error": "UNAUTHORIZED: License Required"}), 403
    batch_id = request.json.get('batch_id')
    if batch_id in ACTIVE_CONFIRMATIONS: return jsonify({"error": "THIS BATCH IS ALREADY RUNNING"}), 429
    raw_cookies = current_user.auth_cookies; raw_csrf = current_user.auth_csrf
    if not raw_cookies or len(raw_cookies) < 10: return jsonify({"error": "MISSING COOKIES"}), 400
    final_cookies, final_csrf = parse_cookies_and_csrf(raw_cookies)
    real_csrf = final_csrf if final_csrf else raw_csrf
    is_valid, msg = validate_session(final_cookies, real_csrf)
    if not is_valid: return jsonify({"error": msg}), 400
    ACTIVE_CONFIRMATIONS.add(batch_id)
    db_path = current_app.config['DB_PATH']
    def task(app_context):
        with app_context:
            try:
                conn = sqlite3.connect(db_path, timeout=30); c = conn.cursor()
                c.execute("UPDATE batches SET status = 'PROCESSING' WHERE batch_id = ?", (batch_id,))
                conn.commit(); conn.close()
                run_confirmation(batch_id, raw_cookies, raw_csrf)
                conn = sqlite3.connect(db_path, timeout=30); c = conn.cursor()
                c.execute("SELECT count, success_count FROM batches WHERE batch_id = ?", (batch_id,))
                conn.close()
            except Exception as e: print(f"[CONFIRM] CRITICAL ERROR: {e}")
            finally:
                if batch_id in ACTIVE_CONFIRMATIONS: ACTIVE_CONFIRMATIONS.remove(batch_id)
    thread = threading.Thread(target=task, args=(current_app._get_current_object().app_context(),)); thread.start()
    return jsonify({"status": "success", "message": "JOB STARTED - MONITORING"})

@main_bp.route('/api/download/csv/<batch_id>')
@login_required
def download_csv(batch_id):
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT filename, status FROM batches WHERE batch_id = ? AND user_id = ?", (batch_id, current_user.id))
    batch_row = c.fetchone()
    
    if not batch_row: 
        conn.close()
        return jsonify({"error": "UNAUTHORIZED"}), 403
    
    # *** PATCH: REFUND CHECK ***
    if batch_row[1] == 'REFUNDED':
        conn.close()
        return jsonify({"error": "ACCESS REVOKED: BATCH REFUNDED"}), 403

    download_name = f"{batch_id}.csv"
    if batch_row and batch_row[0] and '_' in batch_row[0]: download_name = batch_row[0].split('_', 1)[1]
    
    c.execute("SELECT id, from_name, to_name, tracking, created_at, address_to FROM history WHERE batch_id = ?", (batch_id,))
    rows = c.fetchall(); conn.close()
    
    output = io.StringIO(); writer = csv.writer(output)
    writer.writerow(['No', 'Id', 'ClassService', 'FromName', 'ToName', 'TrackingId', 'TransDate', 'AddressTo'])
    for idx, row in enumerate(rows):
        try: fmt_date = (datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S") - timedelta(hours=5)).strftime("%m/%d/%Y %I:%M:%S %p")
        except: fmt_date = row[4]
        writer.writerow([idx + 1, row[0], "USPS Priority", row[1], row[2], row[3], fmt_date, row[5]])
    return Response(output.getvalue(), mimetype="text/csv", headers={"Content-disposition": f"attachment; filename={download_name}"})

@main_bp.route('/api/download/pdf/<batch_id>')
@login_required
def download_pdf(batch_id):
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT filename, status FROM batches WHERE batch_id = ? AND user_id = ?", (batch_id, current_user.id))
    batch_row = c.fetchone()
    conn.close()
    
    if not batch_row: return jsonify({"error": "UNAUTHORIZED ACCESS"}), 403
    
    # *** PATCH: REFUND CHECK ***
    if batch_row[1] == 'REFUNDED': return jsonify({"error": "ACCESS REVOKED: BATCH REFUNDED"}), 403

    clean_name = f"Batch_{batch_id}"
    if batch_row and batch_row[0] and '_' in batch_row[0]: clean_name = os.path.splitext(batch_row[0].split('_', 1)[1])[0]
    return send_from_directory(os.path.join(current_app.config['DATA_FOLDER'], 'pdfs'), f"{batch_id}.pdf", as_attachment=True, download_name=f"{clean_name}_{batch_id}.pdf")

@main_bp.route('/api/download/sample-csv')
@login_required
def download_sample_csv():
    output = io.StringIO(); writer = csv.writer(output)
    writer.writerow(STRICT_HEADERS)
    writer.writerow(['1', 'Test LLC', '8823657928', '123 Jump St', 'Company LLC', '', 'New York', 'NY', '10001', 'Ben Dover', '9028439124', '123 Test St', '', '', 'New York', 'NY', '90001', '1', '1', '1', '1', 'desc', 'ref1', 'ref2', 'False', '1/12/2026'])
    return Response(output.getvalue(), mimetype="text/csv", headers={"Content-disposition": "attachment; filename=Bulk_Template.csv"})

@main_bp.route('/api/addresses', methods=['GET'])
@login_required
def get_addresses_list():
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT * FROM sender_addresses WHERE user_id = ?", (current_user.id,))
    data = [{"id": r[0], "name": r[2], "company": r[3], "phone": r[4], "street1": r[5], "street2": r[6], "city": r[7], "state": r[8], "zip": r[9]} for r in c.fetchall()]
    conn.close(); return jsonify(data)

@main_bp.route('/api/addresses', methods=['POST'])
@login_required
def add_new_address():
    d = request.json; conn = get_db(); c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM sender_addresses WHERE user_id = ?", (current_user.id,))
    if c.fetchone()[0] >= 8: conn.close(); return jsonify({"error": "PROFILE LIMIT REACHED (8/8)"}), 400
    c.execute("INSERT INTO sender_addresses (user_id, name, company, street1, street2, city, state, zip, phone) VALUES (?,?,?,?,?,?,?,?,?)", 
              (current_user.id, d['name'], d.get('company',''), d['street1'], d.get('street2', ''), d['city'], d['state'], d['zip'], d['phone']))
    conn.commit(); conn.close(); return jsonify({"status":"success"})

@main_bp.route('/api/addresses/<int:id>', methods=['DELETE'])
@login_required
def delete_single_address(id):
    conn = get_db(); c = conn.cursor()
    c.execute("DELETE FROM sender_addresses WHERE id=? AND user_id=?", (id, current_user.id))
    conn.commit(); conn.close(); return jsonify({"status":"success"})

@main_bp.route('/api/addresses/all', methods=['DELETE'])
@login_required
def delete_all_user_addresses():
    conn = get_db(); c = conn.cursor()
    c.execute("DELETE FROM sender_addresses WHERE user_id=?", (current_user.id,))
    conn.commit(); conn.close(); return jsonify({"status":"success"})