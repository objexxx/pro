import sqlite3
import os

# CONFIG
DB_NAME = "database.db"  # Check your actual DB name in app/__init__.py if different

def repair_db():
    if not os.path.exists(DB_NAME):
        print(f"Database {DB_NAME} not found. Creating...")
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    print("Checking tables...")

    # 1. Users Table
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE,
        password_hash TEXT NOT NULL,
        balance REAL DEFAULT 0.0,
        price_per_label REAL DEFAULT 3.00,
        is_admin INTEGER DEFAULT 0,
        is_banned INTEGER DEFAULT 0,
        api_key TEXT,
        subscription_end TEXT,
        auto_renew INTEGER DEFAULT 0,
        auth_cookies TEXT,
        auth_csrf TEXT,
        auth_url TEXT,
        auth_file_path TEXT,
        inventory_json TEXT,
        default_label_type TEXT DEFAULT 'priority',
        default_version TEXT DEFAULT '95055',
        default_template TEXT DEFAULT 'pitney_v2',
        created_at TEXT
    )''')

    # 2. Batches Table
    c.execute('''CREATE TABLE IF NOT EXISTS batches (
        batch_id TEXT PRIMARY KEY,
        user_id INTEGER,
        filename TEXT,
        count INTEGER,
        success_count INTEGER DEFAULT 0,
        status TEXT DEFAULT 'QUEUED',
        template TEXT,
        version TEXT,
        label_type TEXT,
        created_at TEXT
    )''')

    # 3. History Table (Labels)
    c.execute('''CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT,
        user_id INTEGER,
        ref_id TEXT,
        tracking TEXT,
        status TEXT,
        from_name TEXT,
        to_name TEXT,
        address_to TEXT,
        version TEXT,
        created_at TEXT,
        ref02 TEXT
    )''')

    # 4. Login History
    c.execute('''CREATE TABLE IF NOT EXISTS login_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        ip_address TEXT,
        user_agent TEXT,
        created_at TEXT
    )''')

    # 5. Sender Addresses
    c.execute('''CREATE TABLE IF NOT EXISTS sender_addresses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        name TEXT,
        company TEXT,
        street1 TEXT,
        street2 TEXT,
        city TEXT,
        state TEXT,
        zip TEXT,
        phone TEXT
    )''')

    # 6. User Pricing
    c.execute('''CREATE TABLE IF NOT EXISTS user_pricing (
        user_id INTEGER,
        label_type TEXT,
        version TEXT,
        price REAL,
        PRIMARY KEY (user_id, label_type, version)
    )''')

    # 7. System Config
    c.execute('''CREATE TABLE IF NOT EXISTS system_config (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')

    # --- NEW: Deposit History Table ---
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
    print("Database repair/update complete. 'deposit_history' table is ready.")

if __name__ == "__main__":
    repair_db()