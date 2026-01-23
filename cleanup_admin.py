import sqlite3
import os

DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def wipe_default_admin():
    if not os.path.exists(DB_PATH):
        print("❌ Database not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    try:
        # Check if the insecure 'admin' user exists
        c.execute("SELECT id FROM users WHERE username = 'admin'")
        target = c.fetchone()

        if target:
            # Delete the user 'admin'
            c.execute("DELETE FROM users WHERE username = 'admin'")
            conn.commit()
            print(f"✅ SUCCESS: Deleted insecure user 'admin' (ID: {target[0]}).")
            print("👉 You can now only log in with your new custom Admin credentials.")
        else:
            print("ℹ️  User 'admin' not found. Your database is already clean.")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    wipe_default_admin()