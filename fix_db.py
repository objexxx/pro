import sqlite3
import os

DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def create_table():
    if not os.path.exists(DB_PATH):
        print("❌ Database not found. Run the app first.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("--- 🔧 REPAIRING DATABASE ---")
    
    # Create the missing table
    try:
        c.execute('''CREATE TABLE IF NOT EXISTS deposit_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount REAL,
            currency TEXT,
            txn_id TEXT,
            status TEXT,
            created_at TEXT
        )''')
        print("✅ Table 'deposit_history' created successfully.")
    except Exception as e:
        print(f"❌ Error creating table: {e}")

    # Check if we need to add columns to existing users table (just in case)
    try:
        c.execute("ALTER TABLE users ADD COLUMN default_label_type TEXT DEFAULT 'priority'")
    except: pass

    conn.commit()
    conn.close()
    print("✨ Database is ready for payments.")

if __name__ == "__main__":
    create_table()