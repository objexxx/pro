import requests
import sqlite3
import time
import os

# CONFIG
BASE_URL = "http://127.0.0.1:5000"
DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def simulate_payment():
    print("--- 🧪 SMART PAYMENT SIMULATOR ---")
    
    # 1. Find the REAL User ID
    username = input("Enter your username: ")
    
    if not os.path.exists(DB_PATH):
        print("❌ Database not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM users WHERE username = ?", (username,))
    row = c.fetchone()
    conn.close()

    if not row:
        print(f"❌ User '{username}' not found in database!")
        return

    user_id = row[0]
    print(f"✅ Found User ID: {user_id}")

    amount = 10050.00
    # Create a unique ID that matches your server's format
    track_id = f"TEST_TXN_{int(time.time())}"
    order_id = f"USER_{user_id}_{int(time.time())}_{amount}"

    payload = {
        "status": "Paid",
        "trackId": track_id,
        "orderId": order_id,
        "payAmount": str(amount),
        "currency": "LTC"
    }

    print(f"\n📡 Sending Fake Webhook for ${amount}...")
    
    try:
        r = requests.post(f"{BASE_URL}/api/deposit/webhook", json=payload)
        
        if r.status_code == 200:
            print("✅ SUCCESS! Payment accepted.")
            print("👉 Check your Dashboard. You should see +$50.00 and a 'Paid' history row.")
        else:
            print(f"❌ FAILED. Response: {r.text}")
            print("Note: Ensure you set 'if True:' in routes.py deposit_webhook for this test.")
            
    except Exception as e:
        print(f"Connection Error: {e}")

if __name__ == "__main__":
    simulate_payment()