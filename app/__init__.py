import os
import sqlite3
import threading
import uuid
from flask import Flask
from datetime import datetime
from werkzeug.security import generate_password_hash
from .extensions import login_manager, limiter

def create_app():
    app = Flask(__name__)
    app.secret_key = 'CHANGE_THIS_TO_SUPER_SECRET'
    app.config['VERSION'] = 'v9.0.0' 
    
    app_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(app_dir)
    app.instance_path = os.path.join(app_dir, 'instance')
    app.config['DB_PATH'] = os.path.join(app.instance_path, 'labellab.db')
    app.config['DATA_FOLDER'] = os.path.join(root_dir, 'data')
    
    for folder in [
        app.instance_path,
        app.config['DATA_FOLDER'],
        os.path.join(app.config['DATA_FOLDER'], 'pdfs'),
        os.path.join(app.config['DATA_FOLDER'], 'uploads'),
        os.path.join(app.config['DATA_FOLDER'], 'zpl_templates')
    ]:
        if not os.path.exists(folder): os.makedirs(folder)

    @app.context_processor
    def inject_version():
        return dict(version=app.config['VERSION'])

    init_db(app.config['DB_PATH'])
    login_manager.init_app(app)
    login_manager.login_view = 'main.login'
    limiter.init_app(app)

    from .routes import main_bp
    app.register_blueprint(main_bp)

    # --- FIX: Import Worker INSIDE the function to prevent circular errors ---
    from .worker import start_worker
    start_worker(app)

    return app

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # Users Table
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE, email TEXT, 
        password_hash TEXT, balance REAL DEFAULT 0.0, price_per_label REAL DEFAULT 3.00, 
        is_admin INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0, api_key TEXT,
        subscription_end TEXT, auto_renew INTEGER DEFAULT 0, auth_cookies TEXT,
        auth_csrf TEXT, auth_url TEXT, auth_file_path TEXT, inventory_json TEXT, created_at TEXT
    )''')
    
    # Sender Addresses Table
    c.execute('''CREATE TABLE IF NOT EXISTS sender_addresses (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, 
        name TEXT, company TEXT, phone TEXT, street1 TEXT, street2 TEXT, 
        city TEXT, state TEXT, zip TEXT
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS system_config (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_pricing (user_id INTEGER, label_type TEXT, version TEXT, price REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS batches (batch_id TEXT PRIMARY KEY, user_id INTEGER, filename TEXT, count INTEGER, success_count INTEGER, status TEXT, template TEXT, version TEXT, label_type TEXT, created_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT, user_id INTEGER, ref_id TEXT, tracking TEXT, status TEXT, from_name TEXT, to_name TEXT, address_to TEXT, version TEXT, created_at TEXT)''')

    # Slots
    c.execute("INSERT OR IGNORE INTO system_config (key, value) VALUES (?, ?)", ('slots_monthly_total', '50'))
    c.execute("INSERT OR IGNORE INTO system_config (key, value) VALUES (?, ?)", ('slots_monthly_used', '0'))
    c.execute("INSERT OR IGNORE INTO system_config (key, value) VALUES (?, ?)", ('slots_lifetime_total', '10'))
    c.execute("INSERT OR IGNORE INTO system_config (key, value) VALUES (?, ?)", ('slots_lifetime_used', '0'))
    c.execute("INSERT OR IGNORE INTO system_config (key, value) VALUES (?, ?)", ('system_status', 'OPERATIONAL'))

    c.execute("SELECT * FROM users WHERE username = 'admin'")
    if not c.fetchone():
        hashed = generate_password_hash('admin')
        admin_key = "sk_live_" + str(uuid.uuid4()).replace('-','')[:24]
        c.execute("INSERT INTO users (username, email, password_hash, balance, is_admin, api_key, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                  ('admin', 'admin@labellab.io', hashed, 100000.0, 1, admin_key, datetime.now().strftime("%Y-%m-%d")))
    conn.commit()
    conn.close()

@login_manager.user_loader
def load_user(user_id):
    from .models import User
    return User.get(user_id)