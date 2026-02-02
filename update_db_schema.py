import sqlite3
import os

# Path to your database
DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def update_schema():
    if not os.path.exists(DB_PATH):
        print("❌ Database not found. Please run the app once to create it.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("--- 🛠️  UPDATING DATABASE SCHEMA ---")

    # 1. Create Revenue Ledger Table (For Subscriptions)
    try:
        c.execute('''CREATE TABLE IF NOT EXISTS revenue_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            user_id INTEGER, 
            amount REAL, 
            description TEXT, 
            type TEXT, 
            created_at TEXT
        )''')
        print("✅ 'revenue_ledger' table checked/created.")
    except Exception as e:
        print(f"⚠️ Error creating ledger: {e}")

    # 2. Add 'price' column to batches (For accurate history)
    try:
        c.execute("ALTER TABLE batches ADD COLUMN price REAL")
        print("✅ Added 'price' column to batches table.")
    except sqlite3.OperationalError as e:
        if "duplicate" in str(e):
            print("ℹ️  'price' column already exists in batches.")
        else:
            print(f"❌ Error adding column: {e}")

    conn.commit()
    conn.close()
    print("\n🎉 Database updated successfully!")

if __name__ == "__main__":
    update_schema()