import sqlite3
import os

DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def upgrade():
    if not os.path.exists(DB_PATH): return print("❌ DB not found.")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("--- 📊 ADDING LIFETIME REVENUE LEDGER ---")

    # We need a place to store the $$$ from deleted batches
    try:
        c.execute("INSERT OR IGNORE INTO system_config (key, value) VALUES ('archived_revenue', '0.00')")
        print("✅ Added 'archived_revenue' slot to system_config.")
    except Exception as e: 
        print(f"ℹ️  Error (might already exist): {e}")

    conn.commit()
    conn.close()
    print("\n✨ Database is ready. You can now delete old history without losing revenue stats.")

if __name__ == "__main__":
    upgrade()