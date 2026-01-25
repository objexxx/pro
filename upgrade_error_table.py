import sqlite3
import os

DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def upgrade():
    if not os.path.exists(DB_PATH): return print("❌ DB not found.")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("--- 🛠️ ADDING SERVER ERROR LOGS ---")

    try:
        c.execute('''CREATE TABLE IF NOT EXISTS server_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,       -- e.g., 'Worker', 'LabelEngine', 'Route'
            batch_id TEXT,     -- Optional, if related to a batch
            error_msg TEXT,    -- The actual error text
            created_at TEXT
        )''')
        print("✅ Created 'server_errors' table.")
    except Exception as e: print(f"⚠️  Error: {e}")

    conn.commit()
    conn.close()
    print("\n✨ Ready to capture internal errors.")

if __name__ == "__main__":
    upgrade()