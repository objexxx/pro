import sqlite3
import os

DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def set_slots():
    if not os.path.exists(DB_PATH):
        print("Database not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Force set usage to 1 (since you bought one key)
    c.execute("UPDATE system_config SET value = '1' WHERE key = 'slots_lifetime_used'")
    conn.commit()
    print("✅ Fixed: Lifetime Slots Used set to 1. Dashboard should now show 9 / 10 available.")
    conn.close()

if __name__ == "__main__":
    set_slots()