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

    # 2. Get Credentials
    username = input("Enter Admin Username: ").strip()
    password = input("Enter Admin Password: ").strip()
    
    if not username or not password:
        print("❌ Error: Username and Password required.")
        return

    hashed_pw = generate_password_hash(password)
    api_key = "sk_test_" + secrets.token_hex(16)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # 3. Check if user exists
        c.execute("SELECT id FROM users WHERE username = ?", (username,))
        row = c.fetchone()

        if row:
            # Update existing user to be Admin AND Verified
            c.execute("""
                UPDATE users 
                SET password_hash = ?, is_admin = 1, is_verified = 1 
                WHERE id = ?
            """, (hashed_pw, row[0]))
            print(f"\n✅ UPDATED: User '{username}' is now a Verified Admin.")
        else:
            # Create new Admin (Balance = 0, Verified = 1)
            c.execute("""
                INSERT INTO users (username, email, password_hash, balance, is_admin, api_key, created_at, is_verified) 
                VALUES (?, ?, ?, 0.0, 1, ?, ?, 1)
            """, (username, 'admin@localhost', hashed_pw, api_key, now))
            print(f"\n✅ CREATED: New Verified Admin '{username}'.")

        conn.commit()
    except Exception as e:
        print(f"❌ Database Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    create_admin()