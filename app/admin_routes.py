from flask import Blueprint, render_template, request, jsonify, current_app, abort, Response
from flask_login import login_required, current_user
import sqlite3
import json
import math
import csv
import io
from functools import wraps
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash

# --- 🔒 SECURE URL PREFIX ---
admin_bp = Blueprint('admin', __name__, url_prefix='/x7k9-p2m4-z8q1')

# --- CSRF PROTECTION (SMART MODE) ---
@admin_bp.before_request
def check_csrf_and_origin():
    if request.method == "GET": return
    
    referer = request.headers.get('Referer')
    host = request.host
    
    # Allow localhost/ngrok for dev, enforce matching for prod
    if "127.0.0.1" in host or "localhost" in host: return
    
    if not referer or host not in referer:
        print(f"[CSRF BLOCKED] Host: {host} vs Referer: {referer}")
        return jsonify({"error": "CSRF SECURITY: Invalid Origin"}), 403

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            return abort(403)
        return f(*args, **kwargs)
    return decorated_function

def get_db():
    return sqlite3.connect(current_app.config['DB_PATH'], timeout=30)

# --- SAFE NOTIFICATION HELPER ---
def safe_notify_user(conn, user_id, message, msg_type="info"):
    """Safely attempts to insert a notification, ignoring errors if table is missing."""
    try:
        c = conn.cursor()
        c.execute("INSERT INTO user_notifications (user_id, message, type) VALUES (?, ?, ?)", 
                  (user_id, message, msg_type))
    except Exception as e:
        print(f"[WARNING] Notification failed (Action still succeeded): {e}")

@admin_bp.route('/dashboard')
@login_required
@admin_required
def dashboard():
    return render_template('admin_dashboard.html', user=current_user)

@admin_bp.route('/api/system/health')
@login_required
@admin_required
def system_health():
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT key, value FROM system_config")
    config = dict(c.fetchall())
    is_paused = config.get('worker_paused') == '1'
    last_beat = config.get('worker_last_heartbeat', '')
    
    c.execute("SELECT COUNT(*) FROM batches WHERE status IN ('QUEUED', 'PROCESSING')")
    q = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM batches WHERE status='CONFIRMING'")
    c_active = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM batches WHERE status='FAILED' AND created_at > datetime('now', '-1 day')")
    err_batch = c.fetchone()[0]
    
    # Check Server Errors Count (Safely)
    try:
        c.execute("SELECT COUNT(*) FROM server_errors WHERE created_at > datetime('now', '-1 day')")
        err_sys = c.fetchone()[0]
    except: err_sys = 0
    
    # Lifetime Revenue (Live + Archived)
    c.execute("""SELECT SUM(b.success_count * COALESCE(u.price_per_label, 3.00)) FROM batches b JOIN users u ON b.user_id = u.id WHERE b.status IN ('COMPLETED','PARTIAL')""")
    rev_live = c.fetchone()[0] or 0.0
    rev_arch = float(config.get('archived_revenue', '0.00'))
    rev_life = rev_live + rev_arch
    
    # 30 Day Revenue
    c.execute("""SELECT SUM(b.success_count * COALESCE(u.price_per_label, 3.00)) FROM batches b JOIN users u ON b.user_id = u.id WHERE b.status IN ('COMPLETED','PARTIAL') AND b.created_at > datetime('now', '-30 days')""")
    rev_30 = c.fetchone()[0] or 0.0
    
    # 7-Day Rolling Average for Projection
    c.execute("""SELECT SUM(b.success_count * COALESCE(u.price_per_label, 3.00)) FROM batches b JOIN users u ON b.user_id = u.id WHERE b.status IN ('COMPLETED','PARTIAL') AND b.created_at > datetime('now', '-7 days')""")
    rev_7d = c.fetchone()[0] or 0.0
    
    daily_avg = rev_7d / 7 if rev_7d > 0 else 0.0
    rev_est_30 = daily_avg * 30

    sub_slots_used = int(config.get('slots_monthly_used', 0))
    sub_price = float(config.get('automation_price_monthly', 29.99))
    rev_mrr = sub_slots_used * sub_price

    conn.close()
    return jsonify({
        "worker_status": "PAUSED" if is_paused else "ONLINE", 
        "queue_depth": q, "active_confirmations": c_active, 
        "errors_24h": err_batch + err_sys, "last_heartbeat": last_beat, 
        "revenue_lifetime": rev_life, "revenue_30d": rev_30,
        "revenue_est_30d": rev_est_30, "subs_mrr": rev_mrr, "subs_count": sub_slots_used
    })

@admin_bp.route('/api/server/errors')
@login_required
@admin_required
def server_errors():
    conn = get_db(); c = conn.cursor()
    try:
        c.execute("SELECT id, source, batch_id, error_msg, created_at FROM server_errors ORDER BY created_at DESC LIMIT 100")
        rows = [{"id":r[0], "source":r[1], "batch":r[2], "msg":r[3], "date":r[4]} for r in c.fetchall()]
    except: rows = []
    conn.close()
    return jsonify(rows)

@admin_bp.route('/api/queue/control', methods=['POST'])
@login_required
@admin_required
def queue_control():
    val = '1' if request.json.get('action') == 'pause' else '0'
    conn = get_db(); c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO system_config (key, value) VALUES ('worker_paused', ?)", (val,))
    conn.commit(); conn.close()
    return jsonify({"status": "success"})

@admin_bp.route('/api/jobs/live')
@login_required
@admin_required
def list_live_jobs():
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT b.batch_id, u.username, b.created_at, b.count, b.success_count, b.status FROM batches b JOIN users u ON b.user_id = u.id WHERE b.status IN ('QUEUED', 'PROCESSING') ORDER BY b.created_at ASC")
    rows = [{"id":r[0], "user":r[1], "date":r[2], "size":r[3], "progress":r[4], "status":r[5]} for r in c.fetchall()]
    conn.close()
    return jsonify(rows)

@admin_bp.route('/api/jobs/confirming')
@login_required
@admin_required
def list_confirming_jobs():
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT b.batch_id, u.username, b.created_at, b.count, b.success_count, b.status FROM batches b JOIN users u ON b.user_id = u.id WHERE b.status = 'CONFIRMING' ORDER BY b.created_at ASC")
    rows = [{"id":r[0], "user":r[1], "date":r[2], "size":r[3], "progress":r[4], "status":r[5]} for r in c.fetchall()]
    conn.close()
    return jsonify(rows)

@admin_bp.route('/api/jobs/history')
@login_required
@admin_required
def list_history():
    page = int(request.args.get('page', 1)); limit = 20; offset = (page-1)*limit
    search = request.args.get('search', '').strip()
    conn = get_db(); c = conn.cursor()
    query_base = "FROM batches b JOIN users u ON b.user_id = u.id WHERE b.status NOT IN ('QUEUED', 'PROCESSING', 'CONFIRMING')"
    params = []
    if search:
        query_base += " AND (b.batch_id LIKE ? OR u.username LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
    c.execute(f"SELECT COUNT(*) {query_base}", params); total = c.fetchone()[0]
    c.execute(f"SELECT b.batch_id, u.username, b.created_at, b.count, b.success_count, b.status, u.price_per_label {query_base} ORDER BY b.created_at DESC LIMIT ? OFFSET ?", (*params, limit, offset))
    rows = []
    for r in c.fetchall():
        val = float(r[3]) * float(r[6]) if r[6] else 0.0
        rows.append({"id":r[0], "user":r[1], "date":r[2], "size":r[3], "progress":r[4], "status":r[5], "value": val})
    conn.close()
    return jsonify({"data": rows, "current_page": page, "total_pages": math.ceil(total/limit)})

@admin_bp.route('/api/jobs/action', methods=['POST'])
@login_required
@admin_required
def job_action():
    data = request.json; bid = data.get('batch_id'); act = data.get('action')
    conn = get_db(); c = conn.cursor()
    if act == 'cancel': c.execute("UPDATE batches SET status='FAILED' WHERE batch_id=?", (bid,))
    elif act == 'retry': c.execute("UPDATE batches SET status='QUEUED' WHERE batch_id=?", (bid,))
    elif act == 'refund':
        c.execute("SELECT user_id, count, status FROM batches WHERE batch_id=?", (bid,)); row = c.fetchone()
        if row and row[2] != 'REFUNDED':
            uid, count = row[0], row[1]
            c.execute("SELECT price_per_label FROM users WHERE id=?", (uid,)); p = c.fetchone(); price = p[0] if p else 3.00
            amt = count * price
            c.execute("UPDATE users SET balance = balance + ? WHERE id=?", (amt, uid))
            c.execute("UPDATE batches SET status='REFUNDED' WHERE batch_id=?", (bid,))
            c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", 
                      (current_user.id, "REFUND", f"Batch {bid} refunded (${amt}) - {data.get('reason','Manual')}", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
            
            safe_notify_user(conn, uid, f"REFUND ISSUED: Batch {bid} (${amt:.2f})", "success")
            
            conn.commit(); conn.close()
            return jsonify({"status": "success", "message": f"REFUND BATCH {bid} REFUNDED (${amt:.2f})"})
    
    conn.commit(); conn.close()
    return jsonify({"status": "success", "message": "Action Completed"})

@admin_bp.route('/api/tracking/list')
@login_required
@admin_required
def track_list():
    page = int(request.args.get('page', 1)); limit = 50; offset = (page-1)*limit
    q = request.args.get('search', '').strip()
    prefix = request.args.get('prefix', 'all')
    
    conn = get_db(); c = conn.cursor()
    base = "FROM history h JOIN users u ON h.user_id = u.id WHERE 1=1"
    params = []
    
    if q: 
        base += " AND (h.tracking LIKE ? OR u.username LIKE ?)"
        params.extend([f"%{q}%", f"%{q}%"])
    
    if prefix != 'all':
        base += " AND h.tracking LIKE ?"
        params.append(f"{prefix}%")
    
    c.execute(f"SELECT COUNT(*) {base}", params); total = c.fetchone()[0]
    c.execute(f"SELECT h.tracking, u.username, h.created_at {base} ORDER BY h.created_at DESC LIMIT ? OFFSET ?", (*params, limit, offset))
    rows = [{"tracking":r[0], "user":r[1], "date":r[2]} for r in c.fetchall()]; conn.close()
    return jsonify({"data": rows, "page": page, "pages": math.ceil(total/limit)})

@admin_bp.route('/api/tracking/export')
@login_required
@admin_required
def track_export():
    days = int(request.args.get('days', 30))
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT h.tracking, u.username, h.created_at FROM history h JOIN users u ON h.user_id = u.id WHERE h.created_at > ? ORDER BY h.created_at DESC", (cutoff,))
    rows = c.fetchall(); conn.close()
    out = io.StringIO(); w = csv.writer(out); w.writerow(['Tracking ID', 'Username', 'Date UTC']); w.writerows(rows)
    return Response(out.getvalue(), mimetype='text/csv', headers={"Content-disposition": f"attachment; filename=tracking_export_{days}d.csv"})

@admin_bp.route('/api/users/search', methods=['GET'])
@login_required
@admin_required
def search_users():
    q = request.args.get('q', '').strip(); conn = get_db(); c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users WHERE is_admin = 0"); total_count = c.fetchone()[0]
    if q: c.execute("SELECT id, username, balance, created_at FROM users WHERE username LIKE ? AND is_admin = 0 ORDER BY id DESC LIMIT 50", (f"%{q}%",))
    else: c.execute("SELECT id, username, balance, created_at FROM users WHERE is_admin = 0 ORDER BY id DESC LIMIT 50")
    rows = [{"id":r[0], "username":r[1], "balance":r[2], "date":r[3]} for r in c.fetchall()]; conn.close()
    return jsonify({"users": rows, "total": total_count})

@admin_bp.route('/api/users/details/<int:uid>', methods=['GET'])
@login_required
@admin_required
def user_details(uid):
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT price_per_label, subscription_end, auto_renew FROM users WHERE id=?", (uid,)); u = c.fetchone()
    if not u: conn.close(); return jsonify({}), 404
    
    c.execute("SELECT ip_address FROM login_history WHERE user_id=? ORDER BY created_at DESC LIMIT 1", (uid,)); ip = c.fetchone()
    
    # --- FETCH ALL PRICING VARIANTS ---
    c.execute("SELECT label_type, version, price FROM user_pricing WHERE user_id=?", (uid,))
    prices = {}
    for l, v, p in c.fetchall():
        prices[f"{l}_{v}"] = p
    
    base = u[0]
    # Ensure all keys exist
    for v in ['95055', '94888', '94019', '95888', '91149', '93055']:
        if f'priority_{v}' not in prices: prices[f'priority_{v}'] = base
    
    conn.close()
    return jsonify({
        "ip": ip[0] if ip else "None", 
        "prices": prices, 
        "subscription": {
            "is_active": u[1] and datetime.strptime(u[1], "%Y-%m-%d %H:%M:%S") > datetime.utcnow(), 
            "end_date": u[1] or "--", 
            "auto_renew": bool(u[2])
        }
    })

@admin_bp.route('/api/users/action', methods=['POST'])
@login_required
@admin_required
def user_action():
    data = request.json; act = data.get('action'); uid = data.get('user_id'); conn = get_db(); c = conn.cursor()
    c.execute("SELECT username FROM users WHERE id = ?", (uid,))
    u_row = c.fetchone()
    target_username = u_row[0] if u_row else f"Unknown_ID_{uid}"
    
    msg = "Action Completed"

    if act == 'reset_pass':
        c.execute("UPDATE users SET password_hash=? WHERE id=?", (generate_password_hash(data.get('new_password')), uid))
        msg = f"PASSWORD RESET FOR {target_username}"
        safe_notify_user(conn, uid, "SECURITY ALERT: Your password was reset by admin.", "processing")
    elif act == 'update_price':
        l, v, p = data.get('label_type'), data.get('version'), float(data.get('price'))
        
        # 1. Update/Insert specific row
        c.execute("DELETE FROM user_pricing WHERE user_id=? AND label_type=? AND version=?", (uid, l, v))
        c.execute("INSERT INTO user_pricing (user_id, label_type, version, price) VALUES (?,?,?,?)", (uid, l, v, p))
        
        # 2. Update default price if it matches 95055 (Legacy/Default sync)
        if l == 'priority' and v == '95055':
            c.execute("UPDATE users SET price_per_label=? WHERE id=?", (p, uid))
            
        msg = f"PRICING UPDATED FOR {target_username}"
        safe_notify_user(conn, uid, f"PRICE UPDATE: Your {l} ({v}) rate is now ${p}", "success")
    elif act == 'update_balance':
        amt = float(data.get('amount')); c.execute("UPDATE users SET balance = balance + ? WHERE id=?", (amt, uid)); c.execute("SELECT balance FROM users WHERE id=?", (uid,)); new_bal = c.fetchone()[0]
        c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", (current_user.id, "BALANCE_ADJUST", f"{target_username} Amt {amt}: {data.get('reason')}", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        
        notify_msg = f"BALANCE ADJUSTMENT: ${amt:+.2f}"
        safe_notify_user(conn, uid, notify_msg, "success" if amt > 0 else "error")
        
        conn.commit(); conn.close()
        return jsonify({"status": "success", "new_balance": new_bal, "message": f"BALANCE UPDATED: {target_username} (${amt})"})
    elif act == 'revoke_sub':
        c.execute("UPDATE users SET subscription_end=NULL, auto_renew=0 WHERE id=?", (uid,))
        c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", 
                  (current_user.id, "SUB_REVOKE", f"Revoked {target_username}", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        safe_notify_user(conn, uid, "ALERT: Automation License Revoked.", "error")
        msg = f"LICENSE REVOKED FOR {target_username}"
    elif act == 'grant_sub':
        days = int(data.get('days', 30))
        new_end = (datetime.utcnow() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE users SET subscription_end=?, auto_renew=0 WHERE id=?", (new_end, uid))
        c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", 
                  (current_user.id, "SUB_GRANT", f"Granted {days}d to {target_username}", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        safe_notify_user(conn, uid, f"LICENSE GRANTED: {days} Days Added.", "success")
        msg = f"LICENSE GRANTED TO {target_username} ({days} DAYS)"
    
    conn.commit(); conn.close()
    return jsonify({"status": "success", "message": msg})

@admin_bp.route('/api/logs', methods=['GET'])
@login_required
@admin_required
def get_logs():
    page = int(request.args.get('page', 1)); limit = int(request.args.get('limit', 100)); offset = (page - 1) * limit
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM admin_audit_log"); total = c.fetchone()[0]
    c.execute("SELECT id, action, details, created_at FROM admin_audit_log ORDER BY created_at DESC LIMIT ? OFFSET ?", (limit, offset))
    rows = [{"id":r[0], "action":r[1], "details":r[2], "date":r[3]} for r in c.fetchall()]
    conn.close()
    return jsonify({"data": rows, "pagination": {"current_page": page, "total_pages": math.ceil(total/limit), "total_items": total}})

@admin_bp.route('/api/automation/config', methods=['GET', 'POST'])
@login_required
@admin_required
def automation_config():
    conn = get_db(); c = conn.cursor()
    if request.method == 'POST':
        d = request.json
        for k in ['automation_price_monthly', 'automation_price_lifetime', 'slots_monthly_total', 'slots_lifetime_total']:
            if k in d: c.execute("INSERT OR REPLACE INTO system_config (key, value) VALUES (?, ?)", (k, str(d[k])))
        c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", 
                  (current_user.id, "CONFIG_UPDATE", "Updated Automation Pricing/Slots", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit(); conn.close()
        return jsonify({"status": "success", "message": "SYSTEM CONFIGURATION SAVED"})
    c.execute("SELECT key, value FROM system_config WHERE key IN ('automation_price_monthly', 'automation_price_lifetime', 'slots_monthly_total', 'slots_lifetime_total')")
    data = dict(c.fetchall())
    conn.close()
    return jsonify(data)

# --- NEW: VERSION CONTROL API ---
@admin_bp.route('/api/versions/config', methods=['GET', 'POST'])
@login_required
@admin_required
def version_config():
    conn = get_db(); c = conn.cursor()
    versions = ['95055', '94888', '94019', '95888', '91149', '93055']
    
    if request.method == 'POST':
        action = request.json.get('action')
        
        # 1. Toggle Version Status
        if action == 'toggle_status':
            ver = request.json.get('version')
            enabled = '1' if request.json.get('enabled') else '0'
            c.execute("INSERT OR REPLACE INTO system_config (key, value) VALUES (?, ?)", (f"ver_en_{ver}", enabled))
            c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", 
                      (current_user.id, "VERSION_TOGGLE", f"Version {ver} set to {'ENABLED' if enabled=='1' else 'DISABLED'}", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit(); conn.close()
            return jsonify({"status": "success", "message": f"Version {ver} Updated"})
            
        # 2. Bulk Price Update
        elif action == 'bulk_price':
            ver = request.json.get('version')
            price = float(request.json.get('price'))
            
            # Update Global Config Default
            c.execute("INSERT OR REPLACE INTO system_config (key, value) VALUES (?, ?)", (f"ver_price_{ver}", str(price)))
            
            # Bulk Update All Users
            c.execute("SELECT id FROM users")
            users = c.fetchall()
            
            # Delete existing override and set new one
            c.execute("DELETE FROM user_pricing WHERE version = ?", (ver,))
            for u in users:
                c.execute("INSERT INTO user_pricing (user_id, label_type, version, price) VALUES (?, ?, ?, ?)", (u[0], 'priority', ver, price))
                
                # If it's the main legacy version, update the user column too for backward compatibility
                if ver == '95055':
                    c.execute("UPDATE users SET price_per_label = ? WHERE id = ?", (price, u[0]))
            
            c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", 
                      (current_user.id, "BULK_PRICE", f"Set {ver} to ${price} for ALL users", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
            
            conn.commit(); conn.close()
            return jsonify({"status": "success", "message": f"Updated {ver} price to ${price} for ALL users."})

    # GET
    c.execute("SELECT key, value FROM system_config WHERE key LIKE 'ver_%'")
    rows = dict(c.fetchall())
    config = {}
    for v in versions:
        config[v] = {
            "enabled": rows.get(f"ver_en_{v}", "1") == "1",
            "price": float(rows.get(f"ver_price_{v}", "3.00"))
        }
    conn.close()
    return jsonify(config)