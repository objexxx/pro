import sqlite3
import os

# Path to your database
DB_PATH = os.path.join(os.getcwd(), 'app', 'instance', 'labellab.db')

def fix_users():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found at: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    try:
        print("--- 🔄 FIXING EXISTING USERS ---")
        
        # This command updates EVERY user currently in the database to be Verified
        c.execute("UPDATE users SET is_verified = 1 WHERE is_verified = 0 OR is_verified IS NULL")
        
        count = c.rowcount
        conn.commit()
        
        print(f"✅ SUCCESS: {count} existing users have been marked as VERIFIED.")
        print("🚀 They can now login without an email code.")
        print("🔒 New users registered AFTER this will still require verification.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    fix_users()