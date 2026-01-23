import time
import sqlite3
import os
import threading
import random
import pandas as pd
from datetime import datetime, timedelta
from flask import current_app

def log_debug(message):
    try:
        with open("debug_system.txt", "a") as f:
            f.write(f"[{datetime.now()}] [WORKER] {message}\n")
    except: pass

def get_worker_price(db_path, user_id, label_type, version):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT price FROM user_pricing WHERE user_id = ? AND label_type = ? AND version = ?", (user_id, label_type, version))
    row = c.fetchone()
    if row:
        conn.close()
        return float(row[0])
    c.execute("SELECT price_per_label FROM users WHERE id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return float(row[0]) if row else 3.00

def check_auto_renewals(app):
    try:
        with app.app_context():
            db_path = current_app.config['DB_PATH']
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            c.execute("SELECT id, balance FROM users WHERE auto_renew = 1 AND subscription_end < ?", (now,))
            expired = c.fetchall()
            for uid, bal in expired:
                if bal >= 29.99:
                    new_end = (datetime.utcnow() + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
                    c.execute("UPDATE users SET balance = balance - 29.99, subscription_end = ? WHERE id = ?", (new_end, uid))
                else:
                    c.execute("UPDATE users SET auto_renew = 0 WHERE id = ?", (uid,))
            conn.commit(); conn.close()
    except: pass

def cleanup_old_data(app):
    try:
        with app.app_context():
            cutoff = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
            db_path = current_app.config['DB_PATH']
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            
            c.execute("SELECT batch_id, filename FROM batches WHERE created_at < ?", (cutoff,))
            rows = c.fetchall()
            if rows:
                for bid, fname in rows:
                    try:
                        os.remove(os.path.join(current_app.config['DATA_FOLDER'], 'pdfs', f"{bid}.pdf"))
                        os.remove(os.path.join(current_app.config['DATA_FOLDER'], 'uploads', fname))
                    except: pass
            
            c.execute("DELETE FROM batches WHERE created_at < ?", (cutoff,))
            c.execute("DELETE FROM history WHERE created_at < ?", (cutoff,))
            c.execute("DELETE FROM login_history WHERE created_at < ?", (cutoff,))
            
            conn.commit()
            conn.close()
    except: pass

def select_weighted_batch(cursor):
    cursor.execute("SELECT batch_id, user_id, filename, count, template, version, label_type, created_at FROM batches WHERE status = 'QUEUED'")
    all_queued = cursor.fetchall()
    
    if not all_queued: return None

    user_queues = {}
    for row in all_queued:
        uid = row[1]
        if uid not in user_queues: user_queues[uid] = []
        user_queues[uid].append(row)

    candidates = []
    for uid in user_queues:
        user_queues[uid].sort(key=lambda x: x[7])
        candidates.append(user_queues[uid][0])

    weights = []
    for batch in candidates:
        count = batch[3]
        if count <= 5: w = 100
        elif count <= 20: w = 50
        elif count <= 100: w = 20
        else: w = 5
        weights.append(w)

    return random.choices(candidates, weights=weights, k=1)[0]

def process_queue(app):
    log_debug("Worker Started (Weighted Round-Robin Mode).")
    from .services.label_engine import LabelEngine
    
    with app.app_context():
        try:
            db_path = current_app.config['DB_PATH']
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute("UPDATE batches SET status = 'FAILED' WHERE status = 'PROCESSING'")
            if c.rowcount > 0: log_debug(f"Reset {c.rowcount} stuck batches to FAILED.")
            conn.commit(); conn.close()
        except: pass

    last_cleanup = time.time()
    
    while True:
        try:
            with app.app_context():
                db_path = current_app.config['DB_PATH']
                conn = sqlite3.connect(db_path, timeout=30)
                c = conn.cursor()

                now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                c.execute("INSERT OR REPLACE INTO system_config (key, value) VALUES ('worker_last_heartbeat', ?)", (now_str,))
                conn.commit()

                c.execute("SELECT value FROM system_config WHERE key='worker_paused'")
                paused_row = c.fetchone()
                if paused_row and paused_row[0] == '1':
                    conn.close(); time.sleep(2); continue

                if time.time() - last_cleanup > 3600:
                    check_auto_renewals(app)
                    cleanup_old_data(app)
                    last_cleanup = time.time()

                c.execute("SELECT count(*) FROM batches WHERE status = 'PROCESSING'")
                if c.fetchone()[0] > 0:
                    conn.close(); time.sleep(2); continue

                task = select_weighted_batch(c)

                if task:
                    bid, uid, fname, count, template, version, ltype, created_at = task
                    log_debug(f"Processing Batch {bid} (User: {uid})...")
                    
                    c.execute("UPDATE batches SET status = 'PROCESSING' WHERE batch_id = ?", (bid,))
                    conn.commit()

                    price = get_worker_price(db_path, uid, ltype, version)

                    try:
                        csv_path = os.path.join(current_app.config['DATA_FOLDER'], 'uploads', fname)
                        if not os.path.exists(csv_path): raise Exception("File Missing")

                        # CRITICAL: LOG DATAFRAME HEAD TO SEE ZEROS
                        df = pd.read_csv(csv_path, dtype=str)
                        if 'ZipTo' in df.columns:
                            log_debug(f"Batch {bid} Zip Data (Top 3): {df['ZipTo'].head().tolist()}")
                        
                        engine = LabelEngine()
                        pdf_bytes, success = engine.process_batch(df, ltype, version, bid, db_path, uid, template)

                        if success > 0 and pdf_bytes:
                            with open(os.path.join(current_app.config['DATA_FOLDER'], 'pdfs', f"{bid}.pdf"), 'wb') as f:
                                f.write(pdf_bytes)

                        status = 'COMPLETED'
                        if success == 0: status = 'FAILED'
                        elif success < count: status = 'PARTIAL'

                        failed = count - success
                        if failed > 0:
                            refund = failed * price
                            c.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (refund, uid))
                            log_debug(f"Refunded ${refund} for {failed} failed items.")

                        c.execute("UPDATE batches SET status = ?, success_count = ? WHERE batch_id = ?", (status, success, bid))
                        conn.commit()
                        log_debug(f"Batch {bid} Completed. Success: {success}/{count}")

                    except Exception as e:
                        log_debug(f"CRASH ON BATCH {bid}: {e}")
                        refund = count * price
                        c.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (refund, uid))
                        c.execute("UPDATE batches SET status = 'FAILED', success_count = 0 WHERE batch_id = ?", (bid,))
                        conn.commit()
                
                conn.close()

        except Exception as e:
            print(f"Worker Loop Error: {e}")
        
        time.sleep(2)

def start_worker(app):
    t = threading.Thread(target=process_queue, args=(app,))
    t.daemon = True
    t.start()