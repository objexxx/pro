from flask import Blueprint, render_template, request, jsonify, current_app, abort, Response
from flask_login import login_required, current_user
import sqlite3
import json
import os
import math
import csv
import io
from functools import wraps
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

# --- SECURITY DECORATOR ---
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            return abort(403)
        return f(*args, **kwargs)
    return decorated_function

# --- DB HELPER ---
def get_db():
    return sqlite3.connect(current_app.config['DB_PATH'], timeout=30)

# --- DASHBOARD VIEW ---
@admin_bp.route('/dashboard')
@login_required
@admin_required
def dashboard():
    return render_template('admin_dashboard.html', user=current_user)

# --- SYSTEM HEALTH ---
@admin_bp.route('/api/system/health')
@login_required
@admin_required
def system_health():
    conn = get_db(); c = conn.cursor()
    
    # Worker Status
    c.execute("SELECT value FROM system_config WHERE key='worker_paused'")
    row = c.fetchone(); is_paused = row and row[0] == '1'
    
    # Heartbeat
    c.execute("SELECT value FROM system_config WHERE key='worker_last_heartbeat'")
    hb = c.fetchone(); last = hb[0] if hb else "UNKNOWN"
    
    # Queue Depth (Label Gen) - Fix: Count PROCESSING too
    c.execute("SELECT COUNT(*) FROM batches WHERE status IN ('QUEUED', 'PROCESSING')")
    q = c.fetchone()[0]

    # Active Confirmations
    c.execute("SELECT COUNT(*) FROM batches WHERE status='CONFIRMING'")
    c_active = c.fetchone()[0]
    
    # Errors (24h)
    c.execute("SELECT COUNT(*) FROM batches WHERE status='FAILED' AND created_at > datetime('now', '-1 day')")
    err = c.fetchone()[0]
    
    # Revenue
    c.execute("SELECT SUM(b.success_count * u.price_per_label) FROM batches b JOIN users u ON b.user_id=u.id WHERE b.status IN ('COMPLETED','PARTIAL')")
    rev_life = c.fetchone()[0] or 0.0
    c.execute("SELECT SUM(b.success_count * u.price_per_label) FROM batches b JOIN users u ON b.user_id=u.id WHERE b.status IN ('COMPLETED','PARTIAL') AND b.created_at > datetime('now', '-30 days')")
    rev_30 = c.fetchone()[0] or 0.0

    conn.close()
    return jsonify({
        "worker_status": "PAUSED" if is_paused else "ONLINE", 
        "queue_depth": q, 
        "active_confirmations": c_active,
        "errors_24h": err, 
        "last_heartbeat": last, 
        "revenue_lifetime": rev_life, 
        "revenue_30d": rev_30
    })

@admin_bp.route('/api/queue/control', methods=['POST'])
@login_required
@admin_required
def queue_control():
    val = '1' if request.json.get('action') == 'pause' else '0'
    conn = get_db(); c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO system_config (key, value) VALUES ('worker_paused', ?)", (val,))
    conn.commit(); conn.close()
    return jsonify({"status": "success"})

# --- TAB 1: LIVE QUEUE ---
@admin_bp.route('/api/jobs/live')
@login_required
@admin_required
def list_live_jobs():
    conn = get_db(); c = conn.cursor()
    c.execute("""
        SELECT b.batch_id, u.username, b.created_at, b.count, b.success_count, b.status 
        FROM batches b 
        JOIN users u ON b.user_id = u.id 
        WHERE b.status IN ('QUEUED', 'PROCESSING') 
        ORDER BY b.created_at ASC
    """)
    rows = [{"id":r[0], "user":r[1], "date":r[2], "size":r[3], "progress":r[4], "status":r[5]} for r in c.fetchall()]
    conn.close()
    return jsonify(rows)

# --- TAB 2: LIVE CONFIRMING ---
@admin_bp.route('/api/jobs/confirming')
@login_required
@admin_required
def list_confirming_jobs():
    conn = get_db(); c = conn.cursor()
    c.execute("""
        SELECT b.batch_id, u.username, b.created_at, b.count, b.success_count, b.status 
        FROM batches b 
        JOIN users u ON b.user_id = u.id 
        WHERE b.status = 'CONFIRMING' 
        ORDER BY b.created_at ASC
    """)
    rows = [{"id":r[0], "user":r[1], "date":r[2], "size":r[3], "progress":r[4], "status":r[5]} for r in c.fetchall()]
    conn.close()
    return jsonify(rows)

# --- TAB 3: GLOBAL HISTORY ---
@admin_bp.route('/api/jobs/history')
@login_required
@admin_required
def list_history():
    page = int(request.args.get('page', 1))
    limit = 20
    offset = (page-1)*limit
    search = request.args.get('search', '').strip()
    
    conn = get_db(); c = conn.cursor()
    query_base = "FROM batches b JOIN users u ON b.user_id = u.id WHERE b.status NOT IN ('QUEUED', 'PROCESSING', 'CONFIRMING')"
    params = []
    
    if search:
        query_base += " AND (b.batch_id LIKE ? OR u.username LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
        
    c.execute(f"SELECT COUNT(*) {query_base}", params)
    total = c.fetchone()[0]
    
    c.execute(f"""
        SELECT b.batch_id, u.username, b.created_at, b.count, b.success_count, b.status, u.price_per_label 
        {query_base} 
        ORDER BY b.created_at DESC 
        LIMIT ? OFFSET ?
    """, (*params, limit, offset))
    
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
    
    if act == 'cancel':
        c.execute("UPDATE batches SET status='FAILED' WHERE batch_id=?", (bid,))
    elif act == 'retry':
        c.execute("UPDATE batches SET status='QUEUED' WHERE batch_id=?", (bid,))
    elif act == 'refund':
        c.execute("SELECT user_id, count, status FROM batches WHERE batch_id=?", (bid,))
        row = c.fetchone()
        if row and row[2] != 'REFUNDED':
            uid, count = row[0], row[1]
            c.execute("SELECT price_per_label FROM users WHERE id=?", (uid,))
            p = c.fetchone(); price = p[0] if p else 3.00
            amt = count * price
            c.execute("UPDATE users SET balance = balance + ? WHERE id=?", (amt, uid))
            c.execute("UPDATE batches SET status='REFUNDED' WHERE batch_id=?", (bid,))
            c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", 
                      (current_user.id, "REFUND", f"Batch {bid} refunded (${amt}) - {data.get('reason','Manual')}", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
            
    conn.commit(); conn.close()
    return jsonify({"status": "success"})

# --- TAB 4: TRACKING ---
@admin_bp.route('/api/tracking/list')
@login_required
@admin_required
def track_list():
    page = int(request.args.get('page', 1)); limit = 50; offset = (page-1)*limit
    q = request.args.get('search', '').strip()
    conn = get_db(); c = conn.cursor()
    base = "FROM history h JOIN users u ON h.user_id = u.id"; params = []
    if q: base += " WHERE h.tracking LIKE ? OR u.username LIKE ?"; params.extend([f"%{q}%", f"%{q}%"])
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

# --- TAB 5: USERS (UPDATED) ---
@admin_bp.route('/api/users/search', methods=['GET'])
@login_required
@admin_required
def search_users():
    q = request.args.get('q', '').strip()
    conn = get_db(); c = conn.cursor()
    
    # Get Total Count
    c.execute("SELECT COUNT(*) FROM users")
    total_count = c.fetchone()[0]

    # Get List (Filtered or All)
    if q:
        c.execute("SELECT id, username, balance, created_at FROM users WHERE username LIKE ? ORDER BY id DESC LIMIT 50", (f"%{q}%",))
    else:
        # If no search, return first 50 users
        c.execute("SELECT id, username, balance, created_at FROM users ORDER BY id DESC LIMIT 50")
        
    rows = [{"id":r[0], "username":r[1], "balance":r[2], "date":r[3]} for r in c.fetchall()]
    conn.close()
    return jsonify({"users": rows, "total": total_count})

@admin_bp.route('/api/users/details/<int:uid>', methods=['GET'])
@login_required
@admin_required
def user_details(uid):
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT price_per_label, subscription_end, auto_renew FROM users WHERE id=?", (uid,)); u = c.fetchone()
    if not u: conn.close(); return jsonify({}), 404
    c.execute("SELECT ip_address FROM login_history WHERE user_id=? ORDER BY created_at DESC LIMIT 1", (uid,)); ip = c.fetchone()
    prices = {'priority':u[0], 'ground':u[0], 'express':u[0]}
    c.execute("SELECT label_type, price FROM user_pricing WHERE user_id=? AND version='95055'", (uid,))
    for l, p in c.fetchall(): prices[l] = p
    conn.close()
    return jsonify({"ip": ip[0] if ip else "None", "prices": prices, "subscription": {"is_active": u[1] and datetime.strptime(u[1], "%Y-%m-%d %H:%M:%S") > datetime.utcnow(), "end_date": u[1] or "--", "auto_renew": bool(u[2])}})

@admin_bp.route('/api/users/action', methods=['POST'])
@login_required
@admin_required
def user_action():
    data = request.json; act = data.get('action'); uid = data.get('user_id'); conn = get_db(); c = conn.cursor()
    if act == 'reset_pass':
        c.execute("UPDATE users SET password_hash=? WHERE id=?", (generate_password_hash(data.get('new_password')), uid))
    elif act == 'update_price':
        l, p = data.get('label_type'), float(data.get('price'))
        for v in ['95055', '94888']: c.execute("DELETE FROM user_pricing WHERE user_id=? AND label_type=? AND version=?", (uid, l, v)); c.execute("INSERT INTO user_pricing (user_id, label_type, version, price) VALUES (?,?,?,?)", (uid, l, v, p))
        if l == 'priority': c.execute("UPDATE users SET price_per_label=? WHERE id=?", (p, uid))
    elif act == 'update_balance':
        amt = float(data.get('amount')); c.execute("UPDATE users SET balance = balance + ? WHERE id=?", (amt, uid)); c.execute("SELECT balance FROM users WHERE id=?", (uid,)); new_bal = c.fetchone()[0]
        c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", (current_user.id, "BALANCE_ADJUST", f"User {uid} Amt {amt}: {data.get('reason')}", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit(); conn.close(); return jsonify({"status": "success", "new_balance": new_bal})
    elif act == 'revoke_sub':
        c.execute("UPDATE users SET subscription_end=NULL, auto_renew=0 WHERE id=?", (uid,))
    conn.commit(); conn.close(); return jsonify({"status": "success"})