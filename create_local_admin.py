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

    print(f"--- 🛠️  SETUP LOCAL ADMIN (Target: {DB_PATH}) ---")
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 2. Ensure the users table exists (in case you deleted the DB)
    # This matches your models.py structure
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        balance REAL DEFAULT 0.0,
        is_admin BOOLEAN DEFAULT 0,
        api_key TEXT UNIQUE,
        created_at TEXT,
        is_subscribed BOOLEAN DEFAULT 0,
        sub_expires TEXT,
        settings_json TEXT DEFAULT '{}',
        inventory_json TEXT DEFAULT '[]',
        price_per_label REAL DEFAULT 1.00
    )''')

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
            # Update existing user to be Admin with money
            c.execute("""
                UPDATE users 
                SET password_hash = ?, is_admin = 1, balance = 10000 
                WHERE id = ?
            """, (hashed_pw, row[0]))
            print(f"\n✅ UPDATED: User '{username}' is now Admin with $10,000 balance.")
        else:
            # Create new Admin
            c.execute("""
                INSERT INTO users (username, email, password_hash, balance, is_admin, api_key, created_at) 
                VALUES (?, ?, ?, 10000, 1, ?, ?)
            """, (username, 'admin@localhost', hashed_pw, api_key, now))
            print(f"\n✅ CREATED: New Admin '{username}' with $10,000 balance.")

        conn.commit()
    except Exception as e:
        print(f"❌ Database Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    create_admin()