import sqlite3
import os

DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def repair():
    if not os.path.exists(DB_PATH): return print("DB not found.")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("Repairing database tables...")
    
    # Create the error log table
    c.execute('''CREATE TABLE IF NOT EXISTS server_errors (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        source TEXT, 
        batch_id TEXT, 
        error_msg TEXT, 
        created_at TEXT
    )''')
    
    # Create the deposit history table (just in case)
    c.execute('''CREATE TABLE IF NOT EXISTS deposit_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        user_id INTEGER, 
        amount REAL, 
        currency TEXT, 
        txn_id TEXT, 
        status TEXT, 
        created_at TEXT
    )''')

    conn.commit()
    conn.close()
    print("✅ Database repaired.")

if __name__ == "__main__":
    repair()