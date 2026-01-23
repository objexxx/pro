import sqlite3
import time
import os
import pandas as pd
from app.services.label_engine import LabelEngine
from app import create_app

app = create_app()

def process_specific_batch(batch_id):
    with app.app_context():
        db_path = app.config['DB_PATH']
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        
        c.execute("SELECT user_id, filename, count, template, version, label_type FROM batches WHERE batch_id = ?", (batch_id,))
        row = c.fetchone()
        
        if not row:
            print("Batch not found")
            return

        user_id, fname, count, template, version, label_type = row
        print(f"Reprocessing {batch_id}...")

        try:
            csv_path = os.path.join(app.config['DATA_FOLDER'], 'uploads', fname)
            
            # --- FIX APPLIED: Read as text to preserve '07204' ---
            df = pd.read_csv(csv_path, dtype=str)
            
            engine = LabelEngine()
            pdf_bytes, success = engine.process_batch(df, label_type, version, batch_id, db_path, user_id, template)

            if success > 0:
                with open(os.path.join(app.config['DATA_FOLDER'], 'pdfs', f"{batch_id}.pdf"), 'wb') as f:
                    f.write(pdf_bytes)
                c.execute("UPDATE batches SET status='COMPLETED', success_count=? WHERE batch_id=?", (success, batch_id))
                conn.commit()
                print("Done.")
            else:
                print("Failed to generate labels.")
        except Exception as e:
            print(f"Error: {e}")
        
        conn.close()

if __name__ == "__main__":
    bid = input("Enter Batch ID to retry: ")
    process_specific_batch(bid)