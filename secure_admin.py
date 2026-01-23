import sqlite3
import os
import secrets
from werkzeug.security import generate_password_hash

# Path to your database
DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def update_admin():
    if not os.path.exists(DB_PATH):
        print(f"❌ Error: Database not found at {DB_PATH}")
        return

    print("--- 🔒 SECURE ADMIN CREDENTIAL UPDATE ---")
    new_user = input("Enter new Admin Username: ").strip()
    
    if not new_user:
        print("❌ Username cannot be empty.")
        return

    # Securely getting password (input is visible but local only)
    new_pass = input("Enter new Admin Password: ").strip()
    
    if len(new_pass) < 8:
        print("⚠️  Warning: Password is short. We recommend 12+ characters.")
    
    confirm_pass = input("Confirm Password: ").strip()
    
    if new_pass != confirm_pass:
        print("❌ Passwords do not match.")
        return

    hashed_pw = generate_password_hash(new_pass)
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    try:
        # Check if the 'admin' user exists (id=1 or username='admin')
        c.execute("SELECT id FROM users WHERE is_admin = 1 LIMIT 1")
        row = c.fetchone()
        
        if row:
            admin_id = row[0]
            # Update existing admin
            c.execute("UPDATE users SET username = ?, password_hash = ? WHERE id = ?", (new_user, hashed_pw, admin_id))
            print(f"\n✅ SUCCESS: Admin credentials updated.")
            print(f"   User: {new_user}")
        else:
            # Create if doesn't exist
            api_key = "sk_live_" + secrets.token_hex(16)
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            c.execute("INSERT INTO users (username, email, password_hash, balance, is_admin, api_key, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (new_user, 'admin@localhost', hashed_pw, 0, 1, api_key, now))
            print(f"\n✅ SUCCESS: New Admin account created.")

        conn.commit()
    except Exception as e:
        print(f"❌ Database Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    update_admin()