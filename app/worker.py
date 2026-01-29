import time
import sqlite3
import os
import threading
import random
import pandas as pd
import csv
from datetime import datetime, timedelta
from flask import current_app

# --- ERROR LOGGING HELPER ---
def log_server_error(db_path, source, msg, batch_id=None):
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        c = conn.cursor()
        c.execute("INSERT INTO server_errors (source, batch_id, error_msg, created_at) VALUES (?, ?, ?, ?)", 
                  (source, batch_id, str(msg), datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[META ERROR] Failed to log error: {e}")

def log_debug(message):
    print(f"[{datetime.now()}] [WORKER] {message}")

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

def archive_and_purge(app):
    try:
        with app.app_context():
            db_path = current_app.config['DB_PATH']
            cutoff_date = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d %H:%M:%S")
            
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            
            c.execute("SELECT batch_id, user_id, success_count, filename FROM batches WHERE created_at < ? AND status IN ('COMPLETED', 'PARTIAL', 'FAILED')", (cutoff_date,))
            old_batches = c.fetchall()
            
            if not old_batches:
                conn.close(); return
            
            batch_ids = [b[0] for b in old_batches]
            batch_placeholders = ','.join(['?'] * len(batch_ids))
            
            total_revenue_to_archive = 0.0
            for bid, uid, count, fname in old_batches:
                c.execute("SELECT price_per_label FROM users WHERE id=?", (uid,)); res = c.fetchone(); price = res[0] if res else 3.00
                total_revenue_to_archive += (count * price)
            
            c.execute("UPDATE system_config SET value = CAST((CAST(value AS REAL) + ?) AS TEXT) WHERE key = 'archived_revenue'", (total_revenue_to_archive,))
            
            for bid, _, _, fname in old_batches:
                try: os.remove(os.path.join(current_app.config['DATA_FOLDER'], 'pdfs', f"{bid}.pdf"))
                except: pass
                
                try: 
                    if fname:
                        os.remove(os.path.join(current_app.config['DATA_FOLDER'], 'uploads', fname))
                except: pass
            
            c.execute(f"DELETE FROM history WHERE batch_id IN ({batch_placeholders})", batch_ids)
            c.execute(f"DELETE FROM batches WHERE batch_id IN ({batch_placeholders})", batch_ids)
            conn.commit(); conn.execute("VACUUM"); conn.close()
            
            log_debug(f"Purged {len(batch_ids)} old batches (>14 days).")
            
    except Exception as e:
        print(f"[ARCHIVE ERROR] {e}")

def get_next_batch(db_path, worker_id):
    conn = sqlite3.connect(db_path, timeout=60)
    try:
        conn.execute("BEGIN IMMEDIATE")
        c = conn.cursor()
        c.execute("SELECT batch_id, user_id, filename, count, template, version, label_type, created_at FROM batches WHERE status = 'QUEUED' ORDER BY created_at ASC LIMIT 1")
        row = c.fetchone()
        
        if row:
            batch_id = row[0]
            c.execute("UPDATE batches SET status = 'PROCESSING' WHERE batch_id = ?", (batch_id,))
            conn.commit(); conn.close()
            return row
        else:
            conn.commit(); conn.close()
            return None
    except Exception as e:
        try: conn.rollback(); conn.close() 
        except: pass
        return None

def worker_loop(app, worker_id):
    log_debug(f"Worker Thread {worker_id} Started.")
    from .services.label_engine import LabelEngine
    
    with app.app_context():
        try:
            db_path = current_app.config['DB_PATH']
            conn = sqlite3.connect(db_path)
            conn.execute("PRAGMA journal_mode=WAL;") 
            conn.close()
        except: pass

    time.sleep(worker_id * 0.5)

    while True:
        try:
            data_folder = ""
            db_path = ""
            
            with app.app_context():
                data_folder = current_app.config['DATA_FOLDER']
                db_path = current_app.config['DB_PATH']
                
                if worker_id == 1 and int(time.time()) % 3600 < 5:
                    archive_and_purge(app)
                    time.sleep(5)
                
                conn = sqlite3.connect(db_path)
                c = conn.cursor()
                
                # --- HEARTBEAT UPDATE (FIXED) ---
                # This line ensures the admin panel sees the worker as "ONLINE"
                now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                c.execute("INSERT OR REPLACE INTO system_config (key, value) VALUES ('worker_last_heartbeat', ?)", (now_str,))
                conn.commit()
                # --------------------------------

                c.execute("SELECT value FROM system_config WHERE key='worker_paused'")
                paused = c.fetchone()
                conn.close()
                if paused and paused[0] == '1':
                    time.sleep(5); continue

            task = get_next_batch(db_path, worker_id)

            if task:
                bid, uid, fname, count, template, version, ltype, created_at = task
                log_debug(f"[T{worker_id}] Processing {bid}...")
                
                price = get_worker_price(db_path, uid, ltype, version)

                try:
                    csv_path = os.path.join(data_folder, 'uploads', fname)
                    if not os.path.exists(csv_path): 
                        raise Exception(f"File {fname} missing")

                    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
                    engine = LabelEngine()
                    
                    pdf_bytes, success = engine.process_batch(
                        df, ltype, version, bid, db_path, uid, template, data_folder=data_folder
                    )

                    if success > 0 and pdf_bytes:
                        with open(os.path.join(data_folder, 'pdfs', f"{bid}.pdf"), 'wb') as f: f.write(pdf_bytes)

                    status = 'COMPLETED'
                    if success == 0: status = 'FAILED'
                    elif success < count: status = 'PARTIAL'

                    conn = sqlite3.connect(db_path, timeout=60)
                    c = conn.cursor()
                    failed = count - success
                    if failed > 0:
                        refund = failed * price
                        c.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (refund, uid))
                        if success == 0:
                            log_server_error(db_path, f"Worker-{worker_id}", f"Batch {bid} Failed completely. Refunded ${refund}", bid)

                    c.execute("UPDATE batches SET status = ?, success_count = ? WHERE batch_id = ?", (status, success, bid))
                    conn.commit(); conn.close()
                    
                except Exception as e:
                    log_server_error(db_path, f"Worker-{worker_id}", f"CRASH: {str(e)}", bid)
                    
                    conn = sqlite3.connect(db_path, timeout=60)
                    c = conn.cursor()
                    refund = count * price
                    c.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (refund, uid))
                    c.execute("UPDATE batches SET status = 'FAILED', success_count = 0 WHERE batch_id = ?", (bid,))
                    conn.commit(); conn.close()
            else:
                time.sleep(1) 
        
        except Exception as e:
            print(f"[WORKER CRASH] {e}")
            time.sleep(5)

def start_worker(app):
    for i in range(2):
        t = threading.Thread(target=worker_loop, args=(app, i+1))
        t.daemon = True
        t.start()