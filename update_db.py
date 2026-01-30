import sqlite3
import os

# Ensure this points to your actual database file
DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def fix_database():
    if not os.path.exists(DB_PATH):
        print(f"ERROR: Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("Checking database structure...")
    
    # 1. Create User Notifications Table (Fixes 'Action Failed' on balance update)
    try:
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                message TEXT,
                type TEXT DEFAULT 'info',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("✔ 'user_notifications' table verified.")
    except Exception as e:
        print(f"❌ Error creating notifications table: {e}")

    # 2. Ensure Admin Audit Log exists (Just in case)
    try:
        c.execute('''
            CREATE TABLE IF NOT EXISTS admin_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                action TEXT,
                details TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("✔ 'admin_audit_log' table verified.")
    except Exception as e:
        print(f"❌ Error creating audit log: {e}")

    conn.commit()
    conn.close()
    print("\nDONE. Please restart your server (flask run) and try again.")

if __name__ == "__main__":
    fix_database()