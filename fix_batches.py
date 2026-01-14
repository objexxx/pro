import sqlite3
import os

# CORRECTION: Pointing to app/instance/labellab.db
db_path = os.path.join("app", "instance", "labellab.db")

print(f"Looking for database at: {db_path}")

if not os.path.exists(db_path):
    # Fallback check just in case
    if os.path.exists(os.path.join("instance", "labellab.db")):
        db_path = os.path.join("instance", "labellab.db")
        print(f"Found it at alternate path: {db_path}")
    else:
        print(f"❌ ERROR: Still cannot find database at {db_path}")
        print("Please check if the file 'labellab.db' is actually in 'app/instance' or just 'instance'.")
        exit()

try:
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Force all "PROCESSING" batches to "COMPLETED"
    c.execute("UPDATE batches SET status = 'COMPLETED' WHERE status = 'PROCESSING'")
    changes = c.rowcount
    
    conn.commit()
    conn.close()

    print(f"✅ SUCCESS: Unstuck {changes} batches. Refresh your dashboard to see the PDF button.")

except Exception as e:

    print(f"❌ Database Error: {e}")