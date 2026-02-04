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
import requests
import json
import string
from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, send_from_directory, jsonify, redirect, url_for, current_app, Response, send_file
from flask_login import login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from flask_mail import Message # NEW: For Email
from .models import User, get_db, SenderAddress
from .extensions import limiter, mail # NEW: Added mail
from .services import parser 
from .services.label_engine import LabelEngine
from .services.amazon_confirmer import run_confirmation, parse_cookies_and_csrf, validate_session

main_bp = Blueprint('main', __name__)

# --- CONFIGURATION ---
STRICT_HEADERS = [
    'No', 'FromName', 'PhoneFrom', 'Street1From', 'CompanyFrom', 'Street2From', 
    'CityFrom', 'StateFrom', 'PostalCodeFrom', 'ToName', 'PhoneTo', 'Street1To', 
    'Company2', 'Street2To', 'CityTo', 'StateTo', 'ZipTo', 'Weight', 'Length', 
    'Width', 'Height', 'Description', 'Ref01', 'Ref02', 'Contains Hazard', 'Shipment Date'
]

# SECURITY: LOAD FROM ENV
OXAPAY_KEY = os.getenv('OXAPAY_KEY')
ACTIVE_CONFIRMATIONS = set() 

# --- SECURITY LOCKS ---
processing_lock = threading.Lock()
address_lock = threading.Lock()

# --- HELPER: LOGGING ---
def log_debug(message):
    print(f"[{datetime.now()}] [ROUTES] {message}")

# --- HELPER: DB CONNECTION ---
def get_db_conn():
    """Opens a DB connection with a high timeout to prevent locking errors."""
    conn = sqlite3.connect(current_app.config['DB_PATH'], timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

# --- HELPER: SEND OTP EMAIL ---
def send_otp_email(user):
    otp = ''.join(random.choices(string.digits, k=6)) # Generate 6 digit code
    
    conn = get_db_conn()
    c = conn.cursor()
    # Save OTP to DB
    c.execute("UPDATE users SET otp_code = ?, otp_created_at = ? WHERE id = ?", 
              (otp, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), user.id))
    conn.commit()
    conn.close()

    try:
        msg = Message(f"Verification Code: {otp}", recipients=[user.email])
        msg.body = f"Your verification code is: {otp}\n\nThis code expires in 10 minutes."
        mail.send(msg)
        return True
    except Exception as e:
        print(f"Email Error: {e}")
        return False

# --- HELPER: DATAFRAME ---
def normalize_dataframe(df):
    df.columns = [str(c).strip() for c in df.columns]
    
    # --- FIX: Check for required columns BEFORE accessing them ---
    required_cols = ['ToName', 'Street1To']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return None, f"Invalid CSV Format. Missing columns: {', '.join(missing_cols)}"

    if 'No' in df.columns:
        if df['No'].isnull().any() or (df['No'] == '').any():
            return None, "Row Error: Missing Order Number in 'No' column."

    # Now safe to access columns
    incomplete_rows = df[df['ToName'].isnull() | (df['ToName'].astype(str).str.strip() == '') | 
                          df['Street1To'].isnull() | (df['Street1To'].astype(str).str.strip() == '')]
    if not incomplete_rows.empty:
        first_error_idx = incomplete_rows.index[0] + 2
        return None, f"Row {first_error_idx} Error: Missing required Recipient Information."

    if 'StateTo' in df.columns:
        df['StateTo'] = df['StateTo'].astype(str).str.strip().str.upper()
        bad_states = df[df['StateTo'].str.len() != 2]
        if not bad_states.empty:
            bad_row = bad_states.index[0] + 2
            bad_val = bad_states.iloc[0]['StateTo']
            return None, f"Row {bad_row} Error: State '{bad_val}' must be a 2-letter code (e.g. 'CA')."

    if 'StateFrom' in df.columns:
        df['StateFrom'] = df['StateFrom'].astype(str).str.strip().str.upper()
        bad_from = df[df['StateFrom'].str.len() != 2]
        if not bad_from.empty:
             bad_row = bad_from.index[0] + 2
             bad_val = bad_from.iloc[0]['StateFrom']
             return None, f"Row {bad_row} Error: Sender State '{bad_val}' must be 2 letters."

    if 'ZipTo' in df.columns:
        df['ZipTo'] = df['ZipTo'].astype(str).str.split('.').str[0].str.strip()
        short_zips = df[df['ZipTo'].str.len() < 5]
        if not short_zips.empty:
            bad_row = short_zips.index[0] + 2
            bad_val = short_zips.iloc[0]['ZipTo']
            return None, f"Row {bad_row} Error: Zip Code '{bad_val}' is too short."

    return df, None

def get_system_config():
    conn = get_db_conn(); c = conn.cursor()
    try:
        c.execute("SELECT key, value FROM system_config")
        rows = dict(c.fetchall())
    except: rows = {}
    conn.close()
    return rows

def get_price(user_id, label_type, version, default_price):
    try:
        conn = get_db_conn(); c = conn.cursor()
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

# --- HELPER: CHECK ENABLED VERSIONS ---
def get_enabled_versions():
    conn = get_db_conn(); c = conn.cursor()
    c.execute("SELECT key, value FROM system_config WHERE key LIKE 'ver_en_%'")
    rows = dict(c.fetchall())
    conn.close()
    
    return {
        '95055': rows.get('ver_en_95055', '1') == '1',
        '94888': rows.get('ver_en_94888', '1') == '1',
        '94019': rows.get('ver_en_94019', '1') == '1',
        '95888': rows.get('ver_en_95888', '1') == '1',
        '91149': rows.get('ver_en_91149', '1') == '1',
        '93055': rows.get('ver_en_93055', '1') == '1'
    }

def is_version_enabled(version):
    status = get_enabled_versions()
    return status.get(version, True)

# --- AUTH ROUTES ---
@main_bp.route('/')
def index():
    if current_user.is_authenticated:
        if current_user.is_admin: return redirect(url_for('admin.dashboard'))
        return redirect(url_for('main.purchase'))
    return redirect(url_for('main.login'))

@main_bp.route('/login', methods=['GET', 'POST'])
@limiter.limit("50 per minute")
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        remember = True if request.form.get('remember') else False
        
        user_data = User.get_by_username(username)
        
        if user_data and check_password_hash(user_data[3], password):
            # NEW: Check if verified
            conn = get_db_conn()
            c = conn.cursor()
            c.execute("SELECT is_verified, email FROM users WHERE id = ?", (user_data[0],))
            res = c.fetchone()
            conn.close()
            
            # If is_verified is 0 or NULL
            if not res or not res[0]:
                user_obj = User.get(user_data[0])
                send_otp_email(user_obj) # Resend OTP
                return render_template('verify.html', email=res[1], error="Unverified Device. Code sent.")

            user = User.get(user_data[0]) 
            login_user(user, remember=remember)
            
            try:
                conn = get_db_conn(); c = conn.cursor()
                ip = request.headers.get('X-Forwarded-For', request.remote_addr)
                ua = request.headers.get('User-Agent', '')[:200]
                c.execute("INSERT INTO login_history (user_id, ip_address, user_agent, created_at) VALUES (?, ?, ?, ?)",
                          (user.id, ip, ua, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
                conn.commit(); conn.close()
            except: pass
            
            if user.is_admin: return redirect(url_for('admin.dashboard'))
            return redirect(url_for('main.purchase'))
        return render_template('login.html', error="INVALID CREDENTIALS")
    return render_template('login.html')

@main_bp.route('/register', methods=['GET', 'POST'])
@limiter.limit("5 per minute") # Global limit to stop spam
def register():
    if request.method == 'POST':
        username = request.form['username']; email = request.form['email']; password = request.form['password']
        hashed = generate_password_hash(password)
        
        # 1. Create User (Unverified by default)
        user = User.create(username, email, hashed)
        if user:
            # 2. Send 2FA Code
            if send_otp_email(user):
                return render_template('verify.html', email=email, message="Code sent! Check your inbox.")
            else:
                return render_template('login.html', mode="register", error="EMAIL FAILED")
        else: 
            return render_template('login.html', mode="register", error="USERNAME/EMAIL TAKEN")
    return render_template('login.html', mode="register")

@main_bp.route('/verify', methods=['POST'])
@limiter.limit("10 per minute")
def verify_account():
    email = request.form['email']
    code = request.form['code']
    
    # We must fetch manually because User.get_by_username doesn't expose fields easily via ORM
    conn = get_db_conn(); c = conn.cursor()
    c.execute("SELECT id, otp_code, otp_created_at, is_verified FROM users WHERE email = ?", (email,))
    row = c.fetchone()
    conn.close()
    
    if not row: return render_template('verify.html', error="User not found")
    
    user_id, db_code, db_time, is_ver = row
    
    if is_ver: return redirect(url_for('main.login'))
    
    # Check Code
    if str(db_code) == str(code):
        conn = get_db_conn(); c = conn.cursor()
        c.execute("UPDATE users SET is_verified = 1, otp_code = NULL WHERE id = ?", (user_id,))
        conn.commit(); conn.close()
        
        user = User.get(user_id)
        login_user(user)
        return redirect(url_for('main.purchase'))
    else:
        return render_template('verify.html', email=email, error="Invalid Code")

@main_bp.route('/logout')
@login_required
def logout(): logout_user(); return redirect(url_for('main.login'))

# --- DASHBOARD ROUTES ---
@main_bp.route('/dashboard')
@login_required
def dashboard_root(): return redirect(url_for('main.purchase'))

@main_bp.route('/purchase')
@login_required
def purchase(): 
    # [WALMART] Added addresses for dropdown
    conn = get_db_conn(); c = conn.cursor()
    c.execute("SELECT * FROM sender_addresses WHERE user_id = ?", (current_user.id,))
    rows = c.fetchall()
    conn.close()
    addrs = [{"id": r[0], "name": r[2], "street1": r[5]} for r in rows]
    return render_template('dashboard.html', user=current_user, active_tab='purchase', version_status=get_enabled_versions(), addresses=addrs)

@main_bp.route('/history')
@login_required
def history(): 
    return render_template('dashboard.html', user=current_user, active_tab='history', version_status=get_enabled_versions())

@main_bp.route('/automation')
@login_required
def automation(): 
    sys_config = get_system_config()
    monthly_left = int(sys_config.get('slots_monthly_total', 50)) - int(sys_config.get('slots_monthly_used', 0))
    lifetime_left = int(sys_config.get('slots_lifetime_total', 10)) - int(sys_config.get('slots_lifetime_used', 0))
    p_month = sys_config.get('automation_price_monthly', '29.99')
    p_life = sys_config.get('automation_price_lifetime', '499.00')
    return render_template('dashboard.html', user=current_user, active_tab='automation', monthly_left=monthly_left, lifetime_left=lifetime_left, price_monthly=p_month, price_lifetime=p_life, system_status="OPERATIONAL", version_status=get_enabled_versions())

@main_bp.route('/stats')
@login_required
def stats(): 
    return render_template('dashboard.html', user=current_user, active_tab='stats', version_status=get_enabled_versions())

@main_bp.route('/deposit')
@login_required
def deposit():
    return render_template('dashboard.html', user=current_user, active_tab='deposit', version_status=get_enabled_versions())

@main_bp.route('/settings')
@login_required
def settings(): 
    return render_template('dashboard.html', user=current_user, active_tab='settings', version_status=get_enabled_versions())

@main_bp.route('/addresses')
@login_required
def addresses(): 
    return render_template('dashboard.html', user=current_user, active_tab='addresses', version_status=get_enabled_versions())

# --- API ENDPOINTS ---
@main_bp.route('/api/user')
@login_required
def api_user():
    conn = get_db_conn(); c = conn.cursor()
    c.execute("SELECT version, price FROM user_pricing WHERE user_id = ? AND label_type = 'priority'", (current_user.id,))
    prices = {row[0]: float(row[1]) for row in c.fetchall()}
    conn.close()
    base = current_user.price_per_label
    return jsonify({
        "username": current_user.username, 
        "balance": current_user.balance, 
        "price_per_label": base,
        "prices": { 
            "95055": prices.get('95055', base), 
            "94888": prices.get('94888', base), 
            "94019": prices.get('94019', base),
            "95888": prices.get('95888', base), 
            "91149": prices.get('91149', base), 
            "93055": prices.get('93055', base)
        }
    })

@main_bp.route('/api/notifications/poll')
@login_required
def poll_notifications():
    try:
        conn = get_db_conn(); c = conn.cursor()
        c.execute("SELECT id, message, type FROM user_notifications WHERE user_id = ?", (current_user.id,))
        rows = c.fetchall()
        notifs = [{"id": r[0], "msg": r[1], "type": r[2]} for r in rows]
        if notifs:
            ids = [str(n['id']) for n in notifs]
            c.execute(f"DELETE FROM user_notifications WHERE id IN ({','.join(ids)})")
            conn.commit()
        conn.close()
        return jsonify(notifs)
    except: return jsonify([])

@main_bp.route('/api/settings/defaults', methods=['POST'])
@login_required
def save_defaults():
    data = request.json; current_user.update_defaults(data.get('label_type','priority'), data.get('version','95055'), data.get('template','pitney_v2'))
    return jsonify({"status": "success", "message": "DEFAULTS SAVED"})

@main_bp.route('/api/batches')
@login_required
def api_batches():
    page = int(request.args.get('page', 1)); limit = 10; offset = (page-1)*limit
    search = request.args.get('search', '').strip(); sort_by = request.args.get('sort', 'recent')
    view = request.args.get('view', '') 
    
    conn = get_db_conn(); c = conn.cursor()
    
    if view == 'history':
        query = "SELECT * FROM batches WHERE user_id = ?"
    else:
        query = "SELECT * FROM batches WHERE user_id = ? AND filename NOT LIKE 'WALMART_%'"
        
    params = [current_user.id]
    
    if search: query += " AND (batch_id LIKE ? OR filename LIKE ?)"; params.extend([f"%{search}%", f"%{search}%"])
    query += " ORDER BY created_at ASC" if sort_by == 'oldest' else " ORDER BY count DESC" if sort_by == 'high' else " ORDER BY created_at DESC"
    
    c.execute(query.replace("SELECT *", "SELECT COUNT(*)"), params); total_items = c.fetchone()[0]
    query += " LIMIT ? OFFSET ?"; params.extend([limit, offset])
    c.execute(query, params); rows = c.fetchall(); conn.close()
    
    data = []
    for r in rows:
        b_id=r[0]; est_date=to_est(r[9]); is_exp=False
        try: 
            if (datetime.utcnow() - datetime.strptime(r[9], "%Y-%m-%d %H:%M:%S")).days >= 7: is_exp = True
        except: pass
        
        filename_val = r[2]
        is_walmart_batch = filename_val.startswith("WALMART_")
        clean_display_name = filename_val.split('_',1)[1] if '_' in filename_val else filename_val
        
        data.append({
            "batch_id": b_id, 
            "batch_name": clean_display_name, 
            "count": r[3], 
            "success_count": r[4], 
            "status": r[5], 
            "date": est_date, 
            "is_expired": is_exp,
            "is_walmart": is_walmart_batch
        })
    return jsonify({"data": data, "pagination": {"current_page": page, "total_pages": math.ceil(total_items/limit)}})

@main_bp.route('/api/stats')
@login_required
def api_stats():
    conn = get_db_conn(); c = conn.cursor()
    c.execute("SELECT SUM(success_count) FROM batches WHERE user_id = ?", (current_user.id,)); total = c.fetchone()[0] or 0
    c.execute("SELECT COUNT(*) FROM batches WHERE user_id = ?", (current_user.id,)); batches = c.fetchone()[0] or 0
    c.execute("SELECT COUNT(*) FROM batches WHERE user_id = ? AND status = 'READY'", (current_user.id,)); ready = c.fetchone()[0] or 0
    c.execute("SELECT COUNT(*) FROM batches WHERE user_id = ? AND status = 'PARTIAL'", (current_user.id,)); partial = c.fetchone()[0] or 0
    c.execute("SELECT COUNT(*) FROM batches WHERE user_id = ? AND status IN ('FAILED', 'AUTH_ERROR')", (current_user.id,)); failed = c.fetchone()[0] or 0
    conn.close()
    return jsonify({"total_labels": total, "total_batches": batches, "ready_batches": ready, "partial_batches": partial, "failed_batches": failed})

@main_bp.route('/process', methods=['POST'])
@login_required
@limiter.limit("30 per minute") 
def process():
    if 'file' not in request.files: return jsonify({"error": "No file uploaded"}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({"error": "No selected file"}), 400
    
    upload_mode = request.form.get('upload_mode', 'bulk') 
    req_version = request.form.get('tracking_version')
    if not is_version_enabled(req_version):
        return jsonify({"error": f"SERVICE UNAVAILABLE: Version {req_version} is currently disabled."}), 400

    log_debug(f"Processing Upload: {file.filename} Mode: {upload_mode}")
    price = get_price(current_user.id, request.form.get('label_type'), req_version, current_user.price_per_label)

    # --- WALMART LOGIC ---
    if upload_mode == 'walmart':
        sender_id = request.form.get('sender_id')
        if not sender_id: return jsonify({"error": "Sender Profile Required"}), 400
        sender = SenderAddress.get(sender_id)
        if not sender or sender.user_id != current_user.id: return jsonify({"error": "Invalid Sender"}), 400
        
        data, count, _ = parser.parse_walmart_xlsx(file, sender)
        if count == 0: return jsonify({"error": "No valid rows found in XLSX"}), 400
        
        df = pd.DataFrame(data)
        mapping = {
            'to_name': 'ToName', 'to_phone': 'PhoneTo', 'to_street1': 'Street1To', 'to_street2': 'Street2To', 
            'to_city': 'CityTo', 'to_state': 'StateTo', 'to_zip': 'ZipTo',
            'from_name': 'FromName', 'from_phone': 'PhoneFrom', 'from_street1': 'Street1From', 
            'from_company': 'CompanyFrom', 'from_street2': 'Street2From', 'from_city': 'CityFrom', 
            'from_state': 'StateFrom', 'from_zip': 'PostalCodeFrom',
            'weight': 'Weight', 'reference': 'Description'
        }
        df.rename(columns=mapping, inplace=True)
        
        df['No'] = range(1, len(df) + 1)
        df['Length'] = 10; df['Width'] = 6; df['Height'] = 4
        df['Contains Hazard'] = 'False'
        df['Shipment Date'] = datetime.now().strftime("%m/%d/%Y")
        
        for col in STRICT_HEADERS:
            if col not in df.columns: df[col] = ''
        df = df[STRICT_HEADERS]
        
        file_prefix = "WALMART_"
        file.stream.seek(0)
        
    else:
        try:
            file.stream.seek(0)
            df = pd.read_csv(file, encoding='utf-8-sig', on_bad_lines='skip', dtype=str)
        except Exception as e:
            log_debug(f"CSV Read Fail: {e}")
            return jsonify({"error": "Could not read CSV file. Please ensure it is a valid CSV format."}), 400

        df, error_msg = normalize_dataframe(df)
        if error_msg: return jsonify({"error": error_msg}), 400
        file_prefix = ""

    cost = len(df) * price
    
    # --- SECURITY LOCK START (Prevent Balance Race Conditions) ---
    with processing_lock:
        if not current_user.update_balance(-cost): 
            return jsonify({"error": "INSUFFICIENT FUNDS"}), 402
        
        try:
            batch_id = None
            for _ in range(10): 
                temp_id = str(random.randint(100000, 999999))
                conn = get_db_conn(); c = conn.cursor()
                c.execute("SELECT 1 FROM batches WHERE batch_id = ?", (temp_id,))
                exists = c.fetchone(); conn.close()
                if not exists: batch_id = temp_id; break
            
            if not batch_id:
                current_user.update_balance(cost) 
                return jsonify({"error": "System Busy: Could not allocate Batch ID. Please try again."}), 500

            clean_name = secure_filename(file.filename)
            if not clean_name: clean_name = "upload.csv"
            if clean_name.lower().endswith('.xlsx'): clean_name = clean_name[:-5] + ".csv"
            
            final_filename = f"{file_prefix}{batch_id}_{clean_name}"
            save_path = os.path.join(current_app.config['DATA_FOLDER'], 'uploads', final_filename)
            df.to_csv(save_path, index=False)
            
            if upload_mode == 'walmart':
                orig_name = f"{file_prefix}{batch_id}_ORIG.xlsx"
                orig_path = os.path.join(current_app.config['DATA_FOLDER'], 'uploads', orig_name)
                file.stream.seek(0)
                file.save(orig_path)
            
            conn = get_db_conn(); c = conn.cursor()
            c.execute("INSERT INTO batches (batch_id, user_id, filename, count, success_count, status, template, version, label_type, created_at, price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                      (batch_id, current_user.id, final_filename, len(df), 0, 'QUEUED', request.form.get('template_choice'), req_version, request.form.get('label_type'), datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), price))
            conn.commit(); conn.close()
            log_debug(f"Batch {batch_id} Queued Successfully")
            return jsonify({"status": "success", "batch_id": batch_id})
        except Exception as e:
            log_debug(f"[CRITICAL ERROR] /process endpoint: {str(e)}")
            current_user.update_balance(cost)
            return jsonify({"error": "Internal System Error. Please try again later."}), 500
    # --- SECURITY LOCK END ---

@main_bp.route('/verify-csv', methods=['POST'])
@login_required
def verify_csv():
    if 'file' not in request.files: return jsonify({"error": "No file uploaded"}), 400
    file = request.files['file']
    upload_mode = request.form.get('upload_mode', 'bulk')
    req_version = request.form.get('tracking_version', '95055')
    if not is_version_enabled(req_version): return jsonify({"error": f"SERVICE UNAVAILABLE: Version {req_version} is currently disabled."}), 400
    
    count = 0
    if upload_mode == 'walmart':
        sender_id = request.form.get('sender_id')
        if not sender_id: return jsonify({"error": "Sender Profile Required"}), 400
        sender = SenderAddress.get(sender_id)
        if not sender or sender.user_id != current_user.id: return jsonify({"error": "Invalid Sender"}), 400
        _, count, _ = parser.parse_walmart_xlsx(file, sender)
        if count == 0: return jsonify({"error": "No valid rows found in XLSX"}), 400
    else:
        try: 
            file.stream.seek(0)
            df = pd.read_csv(file, encoding='utf-8-sig', on_bad_lines='skip', dtype=str)
        except Exception as e: return jsonify({"error": "Invalid CSV Format"}), 400
        
        # --- FIXED CALL HERE ---
        df, error_msg = normalize_dataframe(df)
        if error_msg: return jsonify({"error": error_msg}), 400
        count = len(df)

    label_type = request.form.get('label_type', 'priority')
    price = get_price(current_user.id, label_type, req_version, current_user.price_per_label)
    return jsonify({"count": count, "cost": count * price})

# --- DOWNLOAD ROUTES ---
@main_bp.route('/api/download/xlsx/<batch_id>')
@login_required
def download_xlsx(batch_id):
    conn = get_db_conn(); c = conn.cursor()
    c.execute("SELECT filename, status FROM batches WHERE batch_id = ? AND user_id = ?", (batch_id, current_user.id)); batch_row = c.fetchone()
    if not batch_row: conn.close(); return jsonify({"error": "UNAUTHORIZED"}), 403
    if not batch_row[0].startswith("WALMART_"): conn.close(); return jsonify({"error": "NOT A WALMART BATCH"}), 400
    
    c.execute("SELECT ref02, tracking FROM history WHERE batch_id = ? AND status = 'SUCCESS'", (batch_id,))
    tracking_map = {str(row[0]).strip(): row[1] for row in c.fetchall()}
    conn.close()

    orig_name = f"WALMART_{batch_id}_ORIG.xlsx"
    path = os.path.join(current_app.config['DATA_FOLDER'], 'uploads', orig_name)
    if not os.path.exists(path): return jsonify({"error": "ORIGINAL FILE NOT FOUND"}), 404

    try:
        import openpyxl
        wb = openpyxl.load_workbook(path)
        sheet = wb.active

        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=False), start=2):
            try:
                order_num_cell = row[1].value
                qty_cell = row[24].value

                order_id = str(order_num_cell).strip()
                if order_id.endswith('.0'): order_id = order_id[:-2]

                tracking = tracking_map.get(order_id)

                if tracking:
                    sheet.cell(row=row_idx, column=40).value = "Ship"
                    sheet.cell(row=row_idx, column=41).value = qty_cell
                    sheet.cell(row=row_idx, column=42).value = "USPS"
                    sheet.cell(row=row_idx, column=43).value = tracking
                    sheet.cell(row=row_idx, column=44).value = "www.usps.com"
            except Exception:
                continue

        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        return send_file(output, as_attachment=True, download_name=f"Walmart_Fulfilled_{batch_id}.xlsx", mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    except Exception as e:
        print(f"XLSX Gen Error: {e}")
        return jsonify({"error": "FAILED TO GENERATE FILE"}), 500

# --- OXAPAY: VERIFY HELPER ---
def verify_oxapay_payment(track_id):
    try:
        if not OXAPAY_KEY: 
            print("[PAYMENT] Missing OXAPAY_KEY")
            return False, 0.0, "Missing API Key"
            
        url = "https://api.oxapay.com/merchants/inquiry"
        payload = {"merchant": OXAPAY_KEY, "trackId": track_id}
        r = requests.post(url, json=payload, timeout=10)
        data = r.json()
        
        if data.get('result') == 100:
            status = data.get('status', '').lower()
            if status in ['paid', 'complete']: 
                usd_val = float(data.get('amount', 0))
                return True, usd_val, "Paid"
            else:
                return False, 0.0, f"Status: {status.upper()}"
        else:
            return False, 0.0, f"Gateway Error: {data.get('message', 'Unknown')}"
            
    except Exception as e: 
        print(f"[PAYMENT ERROR] {e}")
        return False, 0.0, "Connection Error"

@main_bp.route('/api/deposit/history', methods=['GET'])
@login_required
def get_deposit_history():
    conn = get_db_conn(); c = conn.cursor()
    try: c.execute("UPDATE deposit_history SET status='FAILED' WHERE status='PROCESSING' AND created_at < ?", ((datetime.utcnow()-timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M:%S"),)); conn.commit()
    except: pass
    c.execute("SELECT amount, currency, txn_id, status, created_at FROM deposit_history WHERE user_id = ? ORDER BY id DESC LIMIT 10", (current_user.id,))
    data = [{"amount":r[0],"currency":r[1],"txn_id":r[2],"status":r[3],"date":to_est(r[4])} for r in c.fetchall()]
    conn.close(); return jsonify(data)

@main_bp.route('/api/deposit/check/<txn_id>', methods=['POST'])
@login_required
def manual_check_deposit(txn_id):
    conn = get_db_conn(); c = conn.cursor()
    c.execute("SELECT id, status FROM deposit_history WHERE txn_id = ? AND user_id = ?", (txn_id, current_user.id))
    row = c.fetchone()
    
    if not row: 
        conn.close()
        return jsonify({"error": "Transaction not found"}), 404
        
    if row[1] == 'PAID': 
        conn.close()
        return jsonify({"status": "success", "message": "Already Paid"})
    
    is_valid, paid_amount, status_msg = verify_oxapay_payment(txn_id)
    
    if is_valid and paid_amount > 0:
        current_user.update_balance(paid_amount)
        c.execute("UPDATE deposit_history SET status='PAID', amount=? WHERE id=?", (paid_amount, row[0]))
        conn.commit()
        conn.close()
        return jsonify({"status": "success", "message": f"Payment Confirmed! +${paid_amount} Added."})
    else:
        if status_msg and "Status:" in status_msg:
            clean_status = status_msg.split(': ')[1].strip()
            if clean_status in ['EXPIRED', 'FAILED']:
                c.execute("UPDATE deposit_history SET status=? WHERE id=?", (clean_status, row[0]))
                conn.commit()
        
        conn.close()
        return jsonify({"error": f"Gateway Report: {status_msg}"}), 400

@main_bp.route('/api/deposit/create', methods=['POST'])
@login_required
def create_deposit():
    if not OXAPAY_KEY: return jsonify({"error": "Payment Gateway Configuration Error"}), 503
    data = request.json; usd_amount = float(data.get('amount'))
    if usd_amount < 10: return jsonify({"error": "Minimum deposit is $10"}), 400
    url = "https://api.oxapay.com/merchants/request"
    custom_order_id = f"USER_{current_user.id}_{int(time.time())}_{usd_amount}"
    payload = {"merchant": OXAPAY_KEY, "amount": usd_amount, "currency": "USDT", "lifeTime": 60, "feePaidByPayer": 0, "underPaidCover": 2.0, "callbackUrl": url_for('main.deposit_webhook', _external=True), "returnUrl": url_for('main.purchase', _external=True), "description": f"Deposit ${usd_amount}", "orderId": custom_order_id}
    try:
        r = requests.post(url, json=payload); result = r.json()
        if result.get('result') == 100:
            track_id = result.get('trackId')
            conn = get_db_conn(); c = conn.cursor()
            c.execute("INSERT INTO deposit_history (user_id, amount, currency, txn_id, status, created_at) VALUES (?, ?, ?, ?, ?, ?)", (current_user.id, usd_amount, 'USDT', str(track_id), 'PROCESSING', datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit(); conn.close()
            return jsonify({"status": "success", "pay_link": result.get('payLink')})
        else: return jsonify({"error": "Gateway Unavailable"}), 400
    except: return jsonify({"error": "Connection Error"}), 500

@main_bp.route('/api/deposit/webhook', methods=['POST'])
def deposit_webhook():
    try:
        data = request.json; status = data.get('status', '').lower(); order_id = data.get('orderId'); track_id = data.get('trackId')
        
        if status in ['paid', 'complete']: 
            is_valid, verified_amount, _ = verify_oxapay_payment(track_id)
            
            if is_valid and verified_amount > 0:
                parts = order_id.split('_')
                if len(parts) >= 2:
                    user_id = int(parts[1])
                    user = User.get(user_id)
                    if user:
                        conn = get_db_conn(); c = conn.cursor()
                        c.execute("SELECT id, status FROM deposit_history WHERE txn_id = ?", (str(track_id),))
                        existing = c.fetchone()
                        
                        if existing:
                            if existing[1] != 'PAID':
                                user.update_balance(verified_amount) 
                                c.execute("UPDATE deposit_history SET status='PAID', currency=?, amount=? WHERE id=?", 
                                          (data.get('currency', 'USDT'), verified_amount, existing[0]))
                                conn.commit()
                        else:
                            user.update_balance(verified_amount)
                            c.execute("INSERT INTO deposit_history (user_id, amount, currency, txn_id, status, created_at) VALUES (?, ?, ?, ?, ?, ?)", 
                                      (user_id, verified_amount, data.get('currency', 'USDT'), str(track_id), 'PAID', datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
                            conn.commit()
                        conn.close()
                        return jsonify({"status": "ok"}), 200
        
        elif status in ['expired', 'failed', 'rejected']:
             conn = get_db_conn(); c = conn.cursor()
             c.execute("UPDATE deposit_history SET status='FAILED' WHERE txn_id = ?", (str(track_id),)); conn.commit(); conn.close()
             
    except Exception as e: 
        print(f"[WEBHOOK ERROR] {e}")
        return jsonify({"status": "error"}), 500
    
    return jsonify({"status": "ok"}), 200

# --- AUTOMATION PUBLIC ENDPOINTS ---
@main_bp.route('/api/automation/public_config')
@login_required
def public_automation_config():
    sys_config = get_system_config()
    monthly_left = int(sys_config.get('slots_monthly_total', 50)) - int(sys_config.get('slots_monthly_used', 0))
    lifetime_left = int(sys_config.get('slots_lifetime_total', 10)) - int(sys_config.get('slots_lifetime_used', 0))
    return jsonify({
        "monthly_left": monthly_left,
        "lifetime_left": lifetime_left,
        "monthly_price": sys_config.get('automation_price_monthly', '29.99'),
        "lifetime_price": sys_config.get('automation_price_lifetime', '499.00')
    })

@main_bp.route('/api/automation/save', methods=['POST'])
@login_required
def automation_save():
    if not current_user.is_subscribed: return jsonify({"error": "LICENSE REQUIRED"}), 403
    cookies = request.form.get('cookies', ''); csrf = request.form.get('csrf', '').strip(); inventory = request.form.get('inventory', '')
    current_user.update_settings(cookies, csrf, "", inventory, False)
    return jsonify({"status": "success", "message": "SETTINGS SAVED"})

@main_bp.route('/api/automation/format', methods=['POST'])
@login_required
@limiter.limit("100 per minute")
def automation_format():
    if not current_user.is_subscribed: return jsonify({"error": "LICENSE REQUIRED"}), 403
    file = request.files.get('file')
    if not file: return jsonify({"error": "NO FILE UPLOADED"}), 400
    address_id = request.form.get('address_id'); sender_address = None
    if address_id:
        conn = get_db_conn(); c = conn.cursor()
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
@limiter.limit("120 per minute")
def automation_confirm():
    if not current_user.is_subscribed: return jsonify({"error": "UNAUTHORIZED: License Required"}), 403
    batch_id = request.json.get('batch_id')
    conn = get_db_conn(); c = conn.cursor()
    c.execute("SELECT status FROM batches WHERE batch_id = ? AND user_id = ?", (batch_id, current_user.id)); row = c.fetchone()
    if row and row[0] == 'REFUNDED': conn.close(); return jsonify({'error': 'ACCESS REVOKED: THIS BATCH WAS REFUNDED'}), 403
    conn.close()
    if batch_id in ACTIVE_CONFIRMATIONS: return jsonify({"error": "THIS BATCH IS ALREADY RUNNING"}), 429
    raw_cookies = current_user.auth_cookies; raw_csrf = current_user.auth_csrf
    if not raw_cookies or len(raw_cookies) < 10: return jsonify({"error": "MISSING COOKIES"}), 400
    final_cookies, final_csrf = parse_cookies_and_csrf(raw_cookies); real_csrf = final_csrf if final_csrf else raw_csrf
    is_valid, msg = validate_session(final_cookies, real_csrf)
    if not is_valid: return jsonify({"error": msg}), 400
    ACTIVE_CONFIRMATIONS.add(batch_id); db_path = current_app.config['DB_PATH']
    def task(app_context):
        with app_context:
            try:
                conn = sqlite3.connect(db_path, timeout=30); c = conn.cursor()
                c.execute("UPDATE batches SET status = 'CONFIRMING' WHERE batch_id = ?", (batch_id,)); conn.commit(); conn.close()
                run_confirmation(batch_id, raw_cookies, raw_csrf)
            except Exception as e: print(f"[CONFIRM] CRITICAL ERROR: {e}")
            finally: 
                if batch_id in ACTIVE_CONFIRMATIONS: ACTIVE_CONFIRMATIONS.remove(batch_id)
    thread = threading.Thread(target=task, args=(current_app._get_current_object().app_context(),)); thread.start()
    return jsonify({"status": "success", "message": "JOB STARTED - MONITORING"})

@main_bp.route('/api/download/csv/<batch_id>')
@login_required
def download_csv(batch_id):
    conn = get_db_conn(); c = conn.cursor()
    c.execute("SELECT filename, status FROM batches WHERE batch_id = ? AND user_id = ?", (batch_id, current_user.id)); batch_row = c.fetchone()
    if not batch_row: conn.close(); return jsonify({"error": "UNAUTHORIZED"}), 403
    if batch_row[1] == 'REFUNDED': conn.close(); return jsonify({"error": "ACCESS REVOKED: BATCH REFUNDED"}), 403
    download_name = f"{batch_id}.csv"
    if batch_row and batch_row[0] and '_' in batch_row[0]: download_name = batch_row[0].split('_', 1)[1]
    c.execute("SELECT id, from_name, to_name, tracking, created_at, address_to FROM history WHERE batch_id = ?", (batch_id,)); rows = c.fetchall(); conn.close()
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
    conn = get_db_conn(); c = conn.cursor()
    c.execute("SELECT filename, status FROM batches WHERE batch_id = ? AND user_id = ?", (batch_id, current_user.id)); batch_row = c.fetchone()
    if not batch_row: return jsonify({"error": "UNAUTHORIZED ACCESS"}), 403
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
    conn = get_db_conn(); c = conn.cursor()
    c.execute("SELECT * FROM sender_addresses WHERE user_id = ?", (current_user.id,))
    data = [{"id": r[0], "name": r[2], "company": r[3], "phone": r[4], "street1": r[5], "street2": r[6], "city": r[7], "state": r[8], "zip": r[9]} for r in c.fetchall()]
    conn.close(); return jsonify(data)

@main_bp.route('/api/addresses', methods=['POST'])
@login_required
def add_new_address():
    with address_lock: # Prevent race condition (Spam saving)
        d = request.json; conn = get_db_conn(); c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM sender_addresses WHERE user_id = ?", (current_user.id,)); 
        if c.fetchone()[0] >= 8: conn.close(); return jsonify({"error": "PROFILE LIMIT REACHED (8/8)"}), 400
        c.execute("INSERT INTO sender_addresses (user_id, name, company, street1, street2, city, state, zip, phone) VALUES (?,?,?,?,?,?,?,?,?)", 
                  (current_user.id, d['name'], d.get('company',''), d['street1'], d.get('street2', ''), d['city'], d['state'], d['zip'], d['phone']))
        conn.commit(); conn.close(); return jsonify({"status":"success"})

@main_bp.route('/api/addresses/<int:id>', methods=['DELETE'])
@login_required
def delete_single_address(id):
    conn = get_db_conn(); c = conn.cursor()
    c.execute("DELETE FROM sender_addresses WHERE id=? AND user_id=?", (id, current_user.id)); conn.commit(); conn.close()
    return jsonify({"status":"success"})

@main_bp.route('/api/addresses/all', methods=['DELETE'])
@login_required
def delete_all_user_addresses():
    conn = get_db_conn(); c = conn.cursor()
    c.execute("DELETE FROM sender_addresses WHERE user_id=?", (current_user.id,)); conn.commit(); conn.close()
    return jsonify({"status":"success"})

@main_bp.route('/api/automation/purchase', methods=['POST'])
@login_required
def buy_automation_license():
    data = request.json
    plan = data.get('plan') 
    sys_config = get_system_config()

    if plan == 'lifetime':
        cost = float(sys_config.get('automation_price_lifetime', 499.00))
        days = 36500 
    else:
        cost = float(sys_config.get('automation_price_monthly', 29.99))
        days = 30

    if current_user.balance < cost:
        return jsonify({'error': 'INSUFFICIENT BALANCE'}), 400

    if not current_user.update_balance(-cost):
        return jsonify({'error': 'TRANSACTION FAILED'}), 500

    try:
        conn = get_db_conn()
        c = conn.cursor()
        expiry = (datetime.utcnow() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE users SET subscription_end = ?, is_subscribed = 1 WHERE id = ?", (expiry, current_user.id))
        
        try:
            c.execute("INSERT INTO revenue_ledger (user_id, amount, description, type, created_at) VALUES (?, ?, ?, ?, ?)",
                      (current_user.id, cost, f"Subscription: {plan.upper()}", "SUBSCRIPTION", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        except Exception as led_err:
            print(f"[LEDGER ERROR] {led_err}")

        msg = f"PURCHASE SUCCESSFUL: {plan.upper()} ACCESS ACTIVATED"
        try:
            c.execute("INSERT INTO user_notifications (user_id, message, type, created_at) VALUES (?, ?, ?, ?)", 
                      (current_user.id, msg, 'SUCCESS', datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        except:
            pass 

        slot_key = 'slots_lifetime_used' if plan == 'lifetime' else 'slots_monthly_used'
        c.execute("UPDATE system_config SET value = CAST(value AS INTEGER) + 1 WHERE key = ?", (slot_key,))

        conn.commit()
        conn.close()
        return jsonify({'status': 'success', 'message': 'LICENSE ACTIVATED'})
    except Exception as e:
        log_debug(f"SUB ERROR: {e}")
        return jsonify({'error': 'DATABASE ERROR'}), 500