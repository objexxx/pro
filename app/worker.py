import time
import sqlite3
import os
import threading
import random
import pandas as pd
from datetime import datetime, timedelta
from flask import current_app

# Note: LabelEngine import is moved INSIDE process_queue to prevent circular errors

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
            # USE UTC
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
            # USE UTC
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
                conn.commit()
            conn.close()
    except: pass

# --- NEW QUEUE ALGORITHM ---
def select_weighted_batch(cursor):
    """
    1. Group all QUEUED items by User.
    2. Pick the oldest batch for each user (Candidates).
    3. Weighted random selection favoring smaller counts.
    """
    cursor.execute("SELECT batch_id, user_id, filename, count, template, version, label_type, created_at FROM batches WHERE status = 'QUEUED'")
    all_queued = cursor.fetchall()
    
    if not all_queued:
        return None

    # Step 1: Group by User ID to ensure Round Robin
    user_queues = {}
    for row in all_queued:
        uid = row[1]
        if uid not in user_queues:
            user_queues[uid] = []
        user_queues[uid].append(row)

    # Step 2: Identify Candidates (Oldest Batch Per User)
    candidates = []
    for uid in user_queues:
        # Sort by created_at (index 7) ascending -> First in, First out per user
        user_queues[uid].sort(key=lambda x: x[7])
        candidates.append(user_queues[uid][0])

    # Step 3: Weighted Selection (Small Orders = Higher Weight)
    weights = []
    for batch in candidates:
        count = batch[3]
        
        # Weighting Logic:
        # < 5 labels   = 100 weight (Very High Priority)
        # < 20 labels  = 50 weight
        # < 100 labels = 20 weight
        # > 100 labels = 5 weight (Low Priority, but technically still has a chance)
        if count <= 5:
            w = 100
        elif count <= 20:
            w = 50
        elif count <= 100:
            w = 20
        else:
            w = 5
        weights.append(w)

    # Pick one winner based on weights
    winner = random.choices(candidates, weights=weights, k=1)[0]
    return winner

def process_queue(app):
    print(">> WORKER: Started (Weighted Round-Robin Mode).")
    # --- LAZY IMPORT TO FIX CIRCULAR ERROR ---
    from .services.label_engine import LabelEngine
    
    last_cleanup = time.time()
    
    while True:
        try:
            if time.time() - last_cleanup > 3600:
                check_auto_renewals(app)
                cleanup_old_data(app)
                last_cleanup = time.time()

            with app.app_context():
                db_path = current_app.config['DB_PATH']
                conn = sqlite3.connect(db_path, timeout=30)
                c = conn.cursor()

                # Lock Check (Prevent parallel processing collisions)
                c.execute("SELECT count(*) FROM batches WHERE status = 'PROCESSING'")
                if c.fetchone()[0] > 0:
                    conn.close(); time.sleep(2); continue

                # --- NEW SELECTION LOGIC ---
                task = select_weighted_batch(c)

                if task:
                    bid, uid, fname, count, template, version, ltype, created_at = task
                    print(f">> PROCESSING BATCH {bid} (User: {uid}, Count: {count})...")
                    
                    c.execute("UPDATE batches SET status = 'PROCESSING' WHERE batch_id = ?", (bid,))
                    conn.commit()

                    price = get_worker_price(db_path, uid, ltype, version)

                    try:
                        csv_path = os.path.join(current_app.config['DATA_FOLDER'], 'uploads', fname)
                        if not os.path.exists(csv_path): raise Exception("File Missing")

                        df = pd.read_csv(csv_path)
                        engine = LabelEngine()
                        
                        # --- Pass DB args for Real-Time Updates ---
                        pdf_bytes, success = engine.process_batch(df, ltype, version, bid, db_path, uid, template)

                        if success > 0 and pdf_bytes:
                            with open(os.path.join(current_app.config['DATA_FOLDER'], 'pdfs', f"{bid}.pdf"), 'wb') as f:
                                f.write(pdf_bytes)

                        status = 'COMPLETED'
                        if success == 0: status = 'FAILED'
                        elif success < count: status = 'PARTIAL'

                        # Refund Logic
                        failed = count - success
                        if failed > 0:
                            refund = failed * price
                            c.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (refund, uid))
                            print(f">> REFUNDED ${refund} for {failed} failed items.")

                        c.execute("UPDATE batches SET status = ?, success_count = ? WHERE batch_id = ?", (status, success, bid))
                        conn.commit()

                    except Exception as e:
                        print(f">> CRASH ON BATCH {bid}: {e}")
                        # Full Refund on Crash
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