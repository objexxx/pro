import sqlite3
import os
from werkzeug.security import generate_password_hash
from datetime import datetime

# Adjust DB_PATH if your local setup is different
# If you run this from the main folder, it usually looks for instance/labellab.db
DB_PATH = 'instance/labellab.db' 

if not os.path.exists(DB_PATH):
    # Fallback for some folder structures
    DB_PATH = 'app/instance/labellab.db'

def create_admin():
    if not os.path.exists(DB_PATH):
        print(f"❌ Error: Database not found at {DB_PATH}")
        print("Make sure you run this script from the main project folder.")
        return

    print(f"--- Creating Local Admin Account ---")
    print(f"Target Database: {DB_PATH}")
    
    username = input("Enter Admin Username: ").strip()
    email = input("Enter Admin Email: ").strip()
    password = input("Enter Admin Password: ").strip()
    
    if not username or not email or not password:
        print("❌ Error: All fields are required.")
        return

    hashed_password = generate_password_hash(password)
    created_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        # Insert the new admin user
        # is_admin = 1
        # is_verified = 1 (Bypasses email check)
        # balance = 100.0 (Gives free test balance)
        
        query = """
        INSERT INTO users (
            username, email, password_hash, balance, is_admin, 
            created_at, is_verified, otp_code
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        c.execute(query, (
            username, 
            email, 
            hashed_password, 
            100.00,  # Free balance for testing
            1,       # Make Admin
            created_at,
            1,       # Auto-Verify
            None     # No OTP needed
        ))

        conn.commit()
        conn.close()
        
        print("\n✅ SUCCESS!")
        print(f"Admin User '{username}' created.")
        print("You can now login immediately (No email verification needed).")

    except sqlite3.IntegrityError:
        print(f"\n❌ Error: The username '{username}' or email '{email}' already exists.")
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")

if __name__ == "__main__":
    create_admin()