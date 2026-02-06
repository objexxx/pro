import time
import sqlite3
import os
import threading
import random
import pandas as pd
import csv
from datetime import datetime, timedelta
from flask import current_app
import PyPDF2  
from .services.label_engine import LabelEngine

# --- GLOBAL THREAD LOCK ---
db_lock = threading.Lock()

# --- DATABASE HELPER (EXTREME PATIENCE) ---
def safe_write(db_path, query, args=()):
    """Thread-safe, retry-heavy database writer."""
    with db_lock: 
        retries = 10 
        while retries > 0:
            conn = None
            try:
                conn = sqlite3.connect(db_path, timeout=60)
                conn.execute("PRAGMA journal_mode=WAL") 
                conn.execute("PRAGMA synchronous=NORMAL")
                c = conn.cursor()
                c.execute(query, args)
                conn.commit()
                conn.close()
                return True
            except sqlite3.OperationalError as e:
                if conn:
                    try: conn.close() 
                    except: pass
                
                if "locked" in str(e):
                    time.sleep(random.uniform(0.5, 2.0)) 
                    retries -= 1
                else:
                    print(f"[DB WRITE ERROR] {e}")
                    return False
            except Exception as e:
                if conn:
                    try: conn.close() 
                    except: pass
                print(f"[DB UNKNOWN ERROR] {e}")
                return False
        print(f"[DB FAIL] Could not write to DB after 10 attempts.")
        return False

# --- ERROR LOGGING HELPER ---
def log_server_error(db_path, source, msg, batch_id=None):
    safe_write(db_path, 
               "INSERT INTO server_errors (source, batch_id, error_msg, created_at) VALUES (?, ?, ?, ?)", 
               (source, batch_id, str(msg), datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))

def log_debug(message):
    print(f"[{datetime.now()}] [WORKER] {message}")

def get_worker_price(db_path, user_id, label_type, version):
    try:
        conn = sqlite3.connect(db_path, timeout=60)
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
    except:
        return 3.00

def archive_and_purge(app):
    try:
        with app.app_context():
            db_path = current_app.config['DB_PATH']
            cutoff_date = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d %H:%M:%S")
            
            with db_lock:
                conn = sqlite3.connect(db_path, timeout=60)
                c = conn.cursor()
                c.execute("SELECT batch_id, user_id, success_count, filename FROM batches WHERE created_at < ? AND status IN ('COMPLETED', 'PARTIAL', 'FAILED')", (cutoff_date,))
                old_batches = c.fetchall()
                if not old_batches:
                    conn.close(); return
                
                batch_ids = [b[0] for b in old_batches]
                batch_placeholders = ','.join(['?'] * len(batch_ids))
                
                for bid, _, _, fname in old_batches:
                    try: os.remove(os.path.join(current_app.config['DATA_FOLDER'], 'pdfs', f"{bid}.pdf"))
                    except: pass
                    try: 
                        if fname: os.remove(os.path.join(current_app.config['DATA_FOLDER'], 'uploads', fname))
                    except: pass
                
                c.execute(f"DELETE FROM history WHERE batch_id IN ({batch_placeholders})", batch_ids)
                c.execute(f"DELETE FROM batches WHERE batch_id IN ({batch_placeholders})", batch_ids)
                conn.commit()
                conn.close()
                log_debug(f"Purged {len(batch_ids)} old batches.")
    except Exception as e:
        print(f"[ARCHIVE ERROR] {e}")

def get_next_batch(db_path, worker_id):
    with db_lock:
        try:
            conn = sqlite3.connect(db_path, timeout=60)
            c = conn.cursor()
            
            query = ""
            # --- DEDICATED LANE LOGIC ---
            # Worker 4: ONLY processes Single Labels (Instant Access)
            if worker_id == 4:
                query = """
                    SELECT batch_id, user_id, filename, count, template, version, label_type, created_at 
                    FROM batches 
                    WHERE status = 'QUEUED' AND batch_id LIKE 'SINGLE_%'
                    ORDER BY created_at ASC 
                    LIMIT 1
                """
            else:
                # Workers 1, 2, 3: Process everything, but prioritize Single Labels
                query = """
                    SELECT batch_id, user_id, filename, count, template, version, label_type, created_at 
                    FROM batches 
                    WHERE status = 'QUEUED' 
                    ORDER BY 
                        CASE WHEN batch_id LIKE 'SINGLE_%' THEN 0 ELSE 1 END ASC, 
                        created_at ASC 
                    LIMIT 1
                """
            
            c.execute(query)
            row = c.fetchone()
            
            if row:
                batch_id = row[0]
                c.execute("UPDATE batches SET status = 'PROCESSING' WHERE batch_id = ?", (batch_id,))
                conn.commit(); conn.close()
                return row
            else:
                conn.commit(); conn.close()
                return None
        except Exception:
            return None

def combine_pdfs(batch_id, folder, file_paths):
    try:
        merger = PyPDF2.PdfMerger()
        count = 0
        for path in file_paths:
            if os.path.exists(path):
                merger.append(path)
                count += 1
        
        if count > 0:
            output_path = os.path.join(folder, 'pdfs', f"{batch_id}.pdf")
            merger.write(output_path)
            merger.close()
            return True
    except Exception as e:
        print(f"[WORKER] Merge Failed: {e}")
    return False

def worker_loop(app, worker_id):
    log_debug(f"Worker Thread {worker_id} Started.")
    
    # Initialize Heartbeat Timer
    last_heartbeat_time = 0
    
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
                
                # --- HEARTBEAT CHECK (IDLE) ---
                if time.time() - last_heartbeat_time > 5:
                    safe_write(db_path, "INSERT OR REPLACE INTO system_config (key, value) VALUES ('worker_last_heartbeat', ?)", 
                             (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),))
                    last_heartbeat_time = time.time()

            task = get_next_batch(db_path, worker_id)

            if task:
                bid, uid, fname, count, template, version, ltype, created_at = task
                log_debug(f"[T{worker_id}] Processing {bid}...")
                
                price = get_worker_price(db_path, uid, ltype, version)

                try:
                    csv_path = os.path.join(data_folder, 'uploads', fname)
                    if not os.path.exists(csv_path): raise Exception(f"File {fname} missing")

                    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
                    generated_files = []
                    success = 0
                    
                    history_buffer = [] 
                    buffer_size = 5 
                    
                    for index, row in df.iterrows():
                        # --- HEARTBEAT CHECK (ACTIVE) ---
                        if time.time() - last_heartbeat_time > 5:
                            safe_write(db_path, "INSERT OR REPLACE INTO system_config (key, value) VALUES ('worker_last_heartbeat', ?)", 
                                     (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),))
                            last_heartbeat_time = time.time()

                        try:
                            row_data = row.to_dict()
                            pdf_path, tracking, ref = LabelEngine.create_label(row_data, ltype, version, template, data_folder=data_folder)
                            
                            if tracking and pdf_path:
                                success += 1
                                generated_files.append(pdf_path)
                                
                                history_buffer.append((
                                    bid, uid, ref, tracking, 'SUCCESS', 
                                    row_data.get('FromName',''), row_data.get('ToName',''), row_data.get('Street1To',''), 
                                    version, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), row_data.get('Ref02', '')
                                ))
                                
                            else:
                                print(f"[WORKER] Row {index+1} Skipped (No PDF returned)")
                                
                        except Exception as row_err:
                            print(f"[WORKER] Row {index+1} Failed: {row_err}")
                            
                        # --- FLUSH BUFFER ---
                        if len(history_buffer) >= buffer_size:
                            with db_lock:
                                try:
                                    conn = sqlite3.connect(db_path, timeout=60)
                                    c = conn.cursor()
                                    c.executemany("""
                                        INSERT INTO history (batch_id, user_id, ref_id, tracking, status, from_name, to_name, address_to, version, created_at, ref02)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """, history_buffer)
                                    c.execute("UPDATE batches SET success_count = ? WHERE batch_id = ?", (success, bid))
                                    conn.commit()
                                    conn.close()
                                    history_buffer = [] 
                                except Exception as e:
                                    print(f"[DB FLUSH FAIL] {e}")

                    # --- FLUSH REMAINING ---
                    if history_buffer:
                        safe_write(db_path, "UPDATE batches SET success_count = ? WHERE batch_id = ?", (success, bid))
                        with db_lock:
                            try:
                                conn = sqlite3.connect(db_path, timeout=60)
                                c = conn.cursor()
                                c.executemany("""
                                    INSERT INTO history (batch_id, user_id, ref_id, tracking, status, from_name, to_name, address_to, version, created_at, ref02)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, history_buffer)
                                conn.commit(); conn.close()
                            except: pass

                    if generated_files:
                        combine_pdfs(bid, data_folder, generated_files)
                        for f in generated_files:
                            try: os.remove(f)
                            except: pass

                    status = 'COMPLETED'
                    if success == 0: status = 'FAILED'
                    elif success < count: status = 'PARTIAL'

                    failed_count = count - success
                    
                    if failed_count > 0:
                        refund = failed_count * price
                        safe_write(db_path, "UPDATE users SET balance = balance + ? WHERE id = ?", (refund, uid))
                        if success == 0:
                            log_server_error(db_path, f"Worker-{worker_id}", f"Batch {bid} Failed completely. Refunded ${refund}", bid)

                    safe_write(db_path, "UPDATE batches SET status = ?, success_count = ? WHERE batch_id = ?", (status, success, bid))
                    
                except Exception as e:
                    log_server_error(db_path, f"Worker-{worker_id}", f"CRASH: {str(e)}", bid)
                    refund = count * price
                    safe_write(db_path, "UPDATE users SET balance = balance + ? WHERE id = ?", (refund, uid))
                    safe_write(db_path, "UPDATE batches SET status = 'FAILED', success_count = 0 WHERE batch_id = ?", (bid,))
            else:
                time.sleep(1) 
        
        except Exception as e:
            print(f"[WORKER CRASH] {e}")
            time.sleep(5)

def start_worker(app):
    # --- UPGRADE: 4 THREADS (Thread 4 is Dedicated Single Label Lane) ---
    for i in range(4):
        t = threading.Thread(target=worker_loop, args=(app, i+1))
        t.daemon = True
        t.start()