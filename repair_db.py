import sqlite3
import os

DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def fix():
    if not os.path.exists(DB_PATH): return print("DB not found.")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # 1. Fix Users Table
    cols = {
        "default_label_type": "TEXT DEFAULT 'priority'",
        "default_version": "TEXT DEFAULT '95055'",
        "default_template": "TEXT DEFAULT 'pitney_v2'"
    }
    c.execute("PRAGMA table_info(users)")
    existing = [r[1] for r in c.fetchall()]
    
    for col, definition in cols.items():
        if col not in existing:
            print(f"Adding {col}...")
            try: c.execute(f"ALTER TABLE users ADD COLUMN {col} {definition}")
            except Exception as e: print(f"Error: {e}")

    # 2. Fix Login History
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='login_history'")
    if not c.fetchone():
        print("Creating login_history table...")
        c.execute("CREATE TABLE login_history (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, ip_address TEXT, user_agent TEXT, created_at TEXT)")

    conn.commit()
    conn.close()
    print("Database Repaired.")

if __name__ == "__main__":
    fix()