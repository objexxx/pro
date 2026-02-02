import sqlite3
import os
import secrets
from datetime import datetime
from werkzeug.security import generate_password_hash

# Path to your LOCAL database
DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def create_admin():
    # 1. Ensure the folder exists
    if not os.path.exists(os.path.dirname(DB_PATH)):
        os.makedirs(os.path.dirname(DB_PATH))
        print(f"📁 Created folder: {os.path.dirname(DB_PATH)}")

    print(f"--- 🛠️  SETUP LOCAL ADMIN (NO BALANCE) ---")
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 2. Ensure the users table exists (Matches your app/__init__.py)
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE, email TEXT, 
        password_hash TEXT, balance REAL DEFAULT 0.0, price_per_label REAL DEFAULT 3.00, 
        is_admin INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0, api_key TEXT,
        is_subscribed BOOLEAN DEFAULT 0, subscription_end TEXT, auto_renew INTEGER DEFAULT 0, 
        auth_cookies TEXT, auth_csrf TEXT, auth_url TEXT, auth_file_path TEXT, 
        inventory_json TEXT, created_at TEXT,
        default_label_type TEXT DEFAULT 'priority', 
        default_version TEXT DEFAULT '95055', 
        default_template TEXT DEFAULT 'pitney_v2',
        archived_count INTEGER DEFAULT 0
    )''')
    
    # Ensure notifications table exists
    c.execute('''CREATE TABLE IF NOT EXISTS user_notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, 
        message TEXT, type TEXT, created_at TEXT
    )''')
    
    # Ensure system config exists
    c.execute('''CREATE TABLE IF NOT EXISTS system_config (key TEXT PRIMARY KEY, value TEXT)''')
    
    # 3. Get Credentials
    username = input("Enter Admin Username: ").strip()
    password = input("Enter Admin Password: ").strip()
    
    if not username or not password:
        print("❌ Error: Username and Password required.")
        return

    hashed_pw = generate_password_hash(password)
    api_key = "sk_test_" + secrets.token_hex(16)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # 4. Check if user exists
        c.execute("SELECT id FROM users WHERE username = ?", (username,))
        row = c.fetchone()

        if row:
            # Update existing user to be Admin (Balance untouched)
            c.execute("""
                UPDATE users 
                SET password_hash = ?, is_admin = 1 
                WHERE id = ?
            """, (hashed_pw, row[0]))
            print(f"\n✅ UPDATED: User '{username}' is now an Admin (Balance unchanged).")
        else:
            # Create new Admin (Balance = 0)
            c.execute("""
                INSERT INTO users (username, email, password_hash, balance, is_admin, api_key, created_at) 
                VALUES (?, ?, ?, 0.0, 1, ?, ?)
            """, (username, 'admin@localhost', hashed_pw, api_key, now))
            print(f"\n✅ CREATED: New Admin '{username}' (Balance: $0.00).")

        conn.commit()
    except Exception as e:
        print(f"❌ Database Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    create_admin()