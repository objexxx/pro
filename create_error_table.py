import sqlite3
import os

DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def fix_database():
    if not os.path.exists(DB_PATH):
        print("❌ Database not found. Run the app first.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("--- 🔧 REPAIRING DATABASE ---")
    
    # 1. Create the missing 'server_errors' table
    try:
        c.execute('''CREATE TABLE IF NOT EXISTS server_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            batch_id TEXT,
            error_msg TEXT,
            created_at TEXT
        )''')
        print("✅ Table 'server_errors' created successfully.")
    except Exception as e:
        print(f"⚠️  Error creating table: {e}")

    # 2. Double check 'archived_count' exists (for stats)
    try:
        c.execute("ALTER TABLE users ADD COLUMN archived_count INTEGER DEFAULT 0")
        print("✅ Added 'archived_count' column.")
    except: 
        print("ℹ️  'archived_count' already exists.")

    conn.commit()
    conn.close()
    print("\n✨ Database repaired. Restart your server now.")

if __name__ == "__main__":
    fix_database()