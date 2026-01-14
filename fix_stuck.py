
import sqlite3
import os

# Path to your database
DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def reset_stuck_batches():
    if not os.path.exists(DB_PATH):
        print(f"❌ Error: Database not found at {DB_PATH}")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # 1. Check for stuck batches
        c.execute("SELECT batch_id, batch_name FROM batches WHERE status = 'PROCESSING'")
        stuck_batches = c.fetchall()
        
        if not stuck_batches:
            print("✅ No stuck batches found. System is clean.")
            return

        print(f"⚠️ Found {len(stuck_batches)} stuck batches:")
        for batch in stuck_batches:
            print(f"   - {batch[1]} (ID: {batch[0]})")

        # 2. Reset them to FAILED
        c.execute("UPDATE batches SET status = 'FAILED' WHERE status = 'PROCESSING'")
        conn.commit()
        
        print(f"\n✅ Successfully reset {len(stuck_batches)} batches to 'FAILED'.")
        print("👉 Refresh your dashboard now.")
        
    except Exception as e:
        print(f"❌ Database Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":

    reset_stuck_batches()