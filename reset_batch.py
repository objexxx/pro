import sqlite3
import os

# Path to your database
DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def reset_stuck_batch(batch_id):
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Check current status
    c.execute("SELECT status, count, success_count FROM batches WHERE batch_id = ?", (batch_id,))
    row = c.fetchone()
    
    if not row:
        print(f"❌ Batch {batch_id} not found.")
        return

    print(f"Current Status: {row[0]} | Progress: {row[2]}/{row[1]}")

    # Force reset to FAILED so you can retry in UI
    c.execute("UPDATE batches SET status = 'FAILED' WHERE batch_id = ?", (batch_id,))
    conn.commit()
    conn.close()
    
    print(f"✅ Batch {batch_id} has been reset to FAILED. You can now retry it in the dashboard.")

if __name__ == "__main__":
    # RESET YOUR SPECIFIC BATCH ID HERE
    reset_stuck_batch("423509")