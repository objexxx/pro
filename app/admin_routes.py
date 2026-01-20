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
        # Strict check: Must be authenticated AND have is_admin=1
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

# --- SYSTEM HEALTH & REVENUE METRICS ---
@admin_bp.route('/api/system/health')
@login_required
@admin_required
def system_health():
    conn = get_db(); c = conn.cursor()
    
    # 1. Worker Status
    c.execute("SELECT value FROM system_config WHERE key='worker_paused'")
    row = c.fetchone(); is_paused = row and row[0] == '1'
    
    # 2. Heartbeat
    c.execute("SELECT value FROM system_config WHERE key='worker_last_heartbeat'")
    hb = c.fetchone(); last_beat = hb[0] if hb else "UNKNOWN"
    
    # 3. Queue Depth
    c.execute("SELECT COUNT(*) FROM batches WHERE status='QUEUED'")
    q_depth = c.fetchone()[0]
    
    # 4. Errors (Last 24h)
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("SELECT COUNT(*) FROM batches WHERE status='FAILED' AND created_at > ?", (yesterday,))
    errors_24h = c.fetchone()[0]

    # 5. Revenue Estimation (Completed/Partial Batches Only)
    # Lifetime
    c.execute("""
        SELECT SUM(b.success_count * u.price_per_label) 
        FROM batches b 
        JOIN users u ON b.user_id = u.id 
        WHERE b.status IN ('COMPLETED', 'PARTIAL')
    """)
    rev_life = c.fetchone()[0] or 0.0

    # 30-Day
    month_ago = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""
        SELECT SUM(b.success_count * u.price_per_label) 
        FROM batches b 
        JOIN users u ON b.user_id = u.id 
        WHERE b.status IN ('COMPLETED', 'PARTIAL') 
        AND b.created_at > ?
    """, (month_ago,))
    rev_30 = c.fetchone()[0] or 0.0

    conn.close()
    return jsonify({
        "worker_status": "PAUSED" if is_paused else "ONLINE", 
        "queue_depth": q_depth, 
        "errors_24h": errors_24h, 
        "last_heartbeat": last_beat, 
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

# --- TAB 1: LIVE QUEUE (Active Jobs Only) ---
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

# --- TAB 2: GLOBAL HISTORY (With Search & Value) ---
@admin_bp.route('/api/jobs/history')
@login_required
@admin_required
def list_history():
    page = int(request.args.get('page', 1))
    limit = 20
    offset = (page-1)*limit
    search = request.args.get('search', '').strip()
    
    conn = get_db(); c = conn.cursor()
    
    # Base Query
    query_base = "FROM batches b JOIN users u ON b.user_id = u.id WHERE b.status NOT IN ('QUEUED', 'PROCESSING')"
    params = []
    
    if search:
        query_base += " AND (b.batch_id LIKE ? OR u.username LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
        
    # Get Total Count
    c.execute(f"SELECT COUNT(*) {query_base}", params)
    total = c.fetchone()[0]
    
    # Fetch Data with Price
    c.execute(f"""
        SELECT b.batch_id, u.username, b.created_at, b.count, b.success_count, b.status, u.price_per_label 
        {query_base} 
        ORDER BY b.created_at DESC 
        LIMIT ? OFFSET ?
    """, (*params, limit, offset))
    
    rows = []
    for r in c.fetchall():
        # Calculate Value based on User's Price
        val = float(r[3]) * float(r[6]) if r[6] else 0.0
        rows.append({
            "id": r[0], "user": r[1], "date": r[2], 
            "size": r[3], "progress": r[4], "status": r[5], "value": val
        })
    
    conn.close()
    return jsonify({"data": rows, "current_page": page, "total_pages": math.ceil(total/limit)})

# --- GLOBAL JOB ACTIONS (Fixes Refund Logic) ---
@admin_bp.route('/api/jobs/action', methods=['POST'])
@login_required
@admin_required
def job_action():
    data = request.json
    bid = data.get('batch_id')
    act = data.get('action')
    
    conn = get_db(); c = conn.cursor()
    
    if act == 'cancel':
        c.execute("UPDATE batches SET status='FAILED' WHERE batch_id=?", (bid,))
        
    elif act == 'retry':
        c.execute("UPDATE batches SET status='QUEUED' WHERE batch_id=?", (bid,))
        
    elif act == 'refund':
        # 1. Get Batch Info
        c.execute("SELECT user_id, count, status FROM batches WHERE batch_id=?", (bid,))
        row = c.fetchone()
        
        # Only refund if not already refunded
        if row and row[2] != 'REFUNDED':
            uid, count = row[0], row[1]
            
            # 2. Get User Price
            c.execute("SELECT price_per_label FROM users WHERE id=?", (uid,))
            p_row = c.fetchone()
            price = p_row[0] if p_row else 3.00
            amt = count * price
            
            # 3. Return Funds
            c.execute("UPDATE users SET balance = balance + ? WHERE id=?", (amt, uid))
            
            # 4. Update Batch Status (Locks downloads in routes.py)
            c.execute("UPDATE batches SET status='REFUNDED' WHERE batch_id=?", (bid,))
            
            # 5. Log
            reason = data.get('reason', 'Manual Refund')
            c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", 
                      (current_user.id, "REFUND", f"Batch {bid} refunded (${amt}) - {reason}", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
            
    conn.commit(); conn.close()
    return jsonify({"status": "success"})

# --- TAB 3: ALL TRACKING ---
@admin_bp.route('/api/tracking/list')
@login_required
@admin_required
def track_list():
    page = int(request.args.get('page', 1))
    limit = 50
    offset = (page-1)*limit
    q = request.args.get('search', '').strip()
    
    conn = get_db(); c = conn.cursor()
    
    base = "FROM history h JOIN users u ON h.user_id = u.id"
    params = []
    
    if q:
        base += " WHERE h.tracking LIKE ? OR u.username LIKE ?"
        params.extend([f"%{q}%", f"%{q}%"])
        
    c.execute(f"SELECT COUNT(*) {base}", params)
    total = c.fetchone()[0]
    
    c.execute(f"SELECT h.tracking, u.username, h.created_at {base} ORDER BY h.created_at DESC LIMIT ? OFFSET ?", (*params, limit, offset))
    rows = [{"tracking":r[0], "user":r[1], "date":r[2]} for r in c.fetchall()]
    
    conn.close()
    return jsonify({"data": rows, "page": page, "pages": math.ceil(total/limit)})

@admin_bp.route('/api/tracking/export')
@login_required
@admin_required
def track_export():
    days = int(request.args.get('days', 30))
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    
    conn = get_db(); c = conn.cursor()
    c.execute("""
        SELECT h.tracking, u.username, h.created_at 
        FROM history h 
        JOIN users u ON h.user_id = u.id 
        WHERE h.created_at > ? 
        ORDER BY h.created_at DESC
    """, (cutoff,))
    rows = c.fetchall()
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Tracking ID', 'Username', 'Date UTC'])
    writer.writerows(rows)
    
    return Response(
        output.getvalue(), 
        mimetype='text/csv', 
        headers={"Content-disposition": f"attachment; filename=tracking_export_{days}d.csv"}
    )

# --- TAB 4: USER MANAGEMENT ---
@admin_bp.route('/api/users/search', methods=['GET'])
@login_required
@admin_required
def search_users():
    q = request.args.get('q', '').strip()
    if not q: return jsonify([])
    
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT id, username, balance FROM users WHERE username LIKE ? LIMIT 10", (f"%{q}%",))
    rows = [{"id":r[0], "username":r[1], "balance":r[2]} for r in c.fetchall()]
    conn.close()
    return jsonify(rows)

@admin_bp.route('/api/users/details/<int:uid>', methods=['GET'])
@login_required
@admin_required
def user_details(uid):
    conn = get_db(); c = conn.cursor()
    
    c.execute("SELECT price_per_label, subscription_end, auto_renew FROM users WHERE id=?", (uid,))
    u = c.fetchone()
    if not u: conn.close(); return jsonify({}), 404
    
    c.execute("SELECT ip_address FROM login_history WHERE user_id=? ORDER BY created_at DESC LIMIT 1", (uid,))
    ip = c.fetchone()
    
    prices = {'priority':u[0], 'ground':u[0], 'express':u[0]}
    c.execute("SELECT label_type, price FROM user_pricing WHERE user_id=? AND version='95055'", (uid,))
    for l, p in c.fetchall():
        if l in prices: prices[l] = p
        
    is_active = False
    if u[1]:
        try:
            if datetime.strptime(u[1], "%Y-%m-%d %H:%M:%S") > datetime.utcnow(): is_active = True
        except: pass
        
    conn.close()
    return jsonify({
        "ip": ip[0] if ip else "None", 
        "prices": prices, 
        "subscription": {
            "is_active": is_active, 
            "end_date": u[1] or "--", 
            "auto_renew": bool(u[2])
        }
    })

# --- USER ACTIONS ---
@admin_bp.route('/api/users/action', methods=['POST'])
@login_required
@admin_required
def user_action():
    data = request.json
    act = data.get('action')
    uid = data.get('user_id')
    
    conn = get_db(); c = conn.cursor()
    
    if act == 'reset_pass':
        new_pass = data.get('new_password')
        if not new_pass:
            conn.close()
            return jsonify({"error": "Password required"}), 400
            
        hashed = generate_password_hash(new_pass)
        c.execute("UPDATE users SET password_hash=? WHERE id=?", (hashed, uid))
        c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", 
                  (current_user.id, "PASS_RESET", f"Reset user {uid} password", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        
    elif act == 'update_price':
        ltype = data.get('label_type')
        price = float(data.get('price'))
        
        for v in ['95055', '94888']:
            c.execute("DELETE FROM user_pricing WHERE user_id=? AND label_type=? AND version=?", (uid, ltype, v))
            c.execute("INSERT INTO user_pricing (user_id, label_type, version, price) VALUES (?,?,?,?)", (uid, ltype, v, price))
            
        if ltype == 'priority': 
            c.execute("UPDATE users SET price_per_label=? WHERE id=?", (price, uid))
        
    elif act == 'update_balance':
        amt = float(data.get('amount'))
        reason = data.get('reason', 'Admin Adj')
        c.execute("UPDATE users SET balance = balance + ? WHERE id=?", (amt, uid))
        
        # Log & Return New Balance
        c.execute("SELECT balance FROM users WHERE id=?", (uid,))
        new_bal = c.fetchone()[0]
        c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", 
                  (current_user.id, "BALANCE_ADJUST", f"User {uid} Amt {amt}: {reason}", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        
        conn.commit(); conn.close()
        return jsonify({"status": "success", "new_balance": new_bal})
        
    elif act == 'revoke_sub':
        reason = data.get('reason', 'Admin Revoke')
        c.execute("UPDATE users SET subscription_end=NULL, auto_renew=0 WHERE id=?", (uid,))
        c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", 
                  (current_user.id, "SUB_REVOKE", f"Revoked license User {uid}: {reason}", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        
    conn.commit(); conn.close()
    return jsonify({"status": "success"})