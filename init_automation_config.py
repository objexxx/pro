import sqlite3
import os

DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def init_config():
    if not os.path.exists(DB_PATH):
        print("❌ Database not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Default values
    defaults = [
        ('automation_price_monthly', '29.99'),
        ('automation_price_lifetime', '499.00'),
        ('slots_monthly_total', '50'),
        ('slots_lifetime_total', '10')
    ]
    
    print("--- ⚙️ SETTING AUTOMATION DEFAULTS ---")
    for key, val in defaults:
        try:
            c.execute("INSERT OR IGNORE INTO system_config (key, value) VALUES (?, ?)", (key, val))
            print(f"✅ {key} initialized.")
        except Exception as e:
            print(f"⚠️ Error setting {key}: {e}")

    conn.commit()
    conn.close()
    print("\nDone. You can now configure these in the Admin Panel.")

if __name__ == "__main__":
    init_config()