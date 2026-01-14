# add_balance.py
import sqlite3
import os

# Path to your database
# Since we moved it inside app/instance, we need to point there
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app', 'instance', 'labellab.db')

def add_funds():
    if not os.path.exists(DB_PATH):
        print(f"ERROR: Database not found at {DB_PATH}")
        print("Did you run the app yet?")
        return

    print(f"--- BANK OF LABELLAB ---")
    print(f"Target Database: {DB_PATH}")
    
    username = input("Enter Username to fund: ")
    try:
        amount = float(input("Enter Amount to add (e.g., 50.00): "))
    except ValueError:
        print("Invalid amount.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 1. Check if user exists
    c.execute("SELECT id, balance FROM users WHERE username = ?", (username,))
    user = c.fetchone()

    if not user:
        print(f"❌ User '{username}' not found!")
    else:
        user_id = user[0]
        current_balance = user[1]
        new_balance = current_balance + amount
        
        # 2. Update Balance
        c.execute("UPDATE users SET balance = ? WHERE id = ?", (new_balance, user_id))
        conn.commit()
        print(f"✅ SUCCESS! Added ${amount} to {username}.")
        print(f"💰 New Balance: ${new_balance}")

    conn.close()

if __name__ == "__main__":
    add_funds()