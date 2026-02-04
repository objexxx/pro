import os
import sqlite3
from flask import Flask, request, jsonify, redirect, url_for
from datetime import datetime
from dotenv import load_dotenv 
from werkzeug.middleware.proxy_fix import ProxyFix 
from .extensions import login_manager, limiter, mail, db

# Load .env file
load_dotenv()

def create_app():
    app = Flask(__name__)
    
    # --- SECURITY: FORCE SECRET KEY ---
    app.secret_key = os.getenv('SECRET_KEY') or 'dev_key_for_testing_only'

    app.config['VERSION'] = 'v1.0.2' 
    
    # --- DATABASE SETUP ---
    app_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(app_dir)
    app.instance_path = os.path.join(app_dir, 'instance')
    app.config['DB_PATH'] = os.path.join(app.instance_path, 'labellab.db')
    app.config['DATA_FOLDER'] = os.path.join(root_dir, 'data')

    # --- CRITICAL FIX FOR LOCALHOST LOGIN ---
    # If running on localhost (debug mode), allow cookies over HTTP
    if app.debug:
        app.config['SESSION_COOKIE_SECURE'] = False
        app.config['REMEMBER_COOKIE_SECURE'] = False
    else:
        app.config['SESSION_COOKIE_SECURE'] = True
        app.config['REMEMBER_COOKIE_SECURE'] = True

    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    
    # SQLAlchemy Config (Silences Runtime Errors)
    app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{app.config['DB_PATH']}"
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    # Only enable ProxyFix in production
    if not app.debug:
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
    
    # Ensure directories exist
    for folder in [
        app.instance_path,
        app.config['DATA_FOLDER'],
        os.path.join(app.config['DATA_FOLDER'], 'pdfs'),
        os.path.join(app.config['DATA_FOLDER'], 'uploads'),
        os.path.join(app.config['DATA_FOLDER'], 'zpl_templates')
    ]:
        if not os.path.exists(folder): os.makedirs(folder)

    # --- EMAIL CONFIGURATION (SMTP) ---
    app.config['MAIL_SERVER'] = os.environ.get('MAIL_SERVER', 'smtp.gmail.com')
    app.config['MAIL_PORT'] = int(os.environ.get('MAIL_PORT', 587))
    app.config['MAIL_USE_TLS'] = True
    app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME') 
    app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD') 
    app.config['MAIL_DEFAULT_SENDER'] = os.environ.get('MAIL_USERNAME')

    @app.context_processor
    def inject_version():
        return dict(version=app.config['VERSION'])

    # Initialize Database (Raw SQLite)
    init_db(app.config['DB_PATH'])
    
    # Initialize Extensions
    db.init_app(app) 
    login_manager.init_app(app)
    login_manager.login_view = 'main.login'
    
    # Initialize Rate Limiter
    limiter.init_app(app)
    
    # Initialize Mail
    mail.init_app(app) 
    
    # 429 Error Handler
    @login_manager.unauthorized_handler
    def unauthorized():
        if '/api/' in request.path:
            return jsonify({"error": "Session Expired", "redirect": url_for('main.login')}), 401
        return redirect(url_for('main.login', next=request.url))

    # Register Blueprints
    from .routes import main_bp
    app.register_blueprint(main_bp)

    from .admin_routes import admin_bp
    app.register_blueprint(admin_bp)

    # Start Background Worker
    from .worker import start_worker
    if not app.debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        start_worker(app)

    return app

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # --- UPDATED USERS TABLE WITH 2FA COLUMNS ---
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE, email TEXT, 
        password_hash TEXT, balance REAL DEFAULT 0.0, price_per_label REAL DEFAULT 3.00, 
        is_admin INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0, api_key TEXT,
        is_subscribed BOOLEAN DEFAULT 0, subscription_end TEXT, auto_renew INTEGER DEFAULT 0, 
        auth_cookies TEXT, auth_csrf TEXT, auth_url TEXT, auth_file_path TEXT, 
        inventory_json TEXT, created_at TEXT,
        default_label_type TEXT DEFAULT 'priority', 
        default_version TEXT DEFAULT '95055', 
        default_template TEXT DEFAULT 'pitney_v2',
        archived_count INTEGER DEFAULT 0,
        is_verified BOOLEAN DEFAULT 0,
        otp_code TEXT,
        otp_created_at TEXT
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS sender_addresses (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, 
        name TEXT, company TEXT, phone TEXT, street1 TEXT, street2 TEXT, 
        city TEXT, state TEXT, zip TEXT
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS system_config (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_pricing (user_id INTEGER, label_type TEXT, version TEXT, price REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS batches (batch_id TEXT PRIMARY KEY, user_id INTEGER, filename TEXT, count INTEGER, success_count INTEGER, status TEXT, template TEXT, version TEXT, label_type TEXT, created_at TEXT, price REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT, user_id INTEGER, ref_id TEXT, tracking TEXT, status TEXT, from_name TEXT, to_name TEXT, address_to TEXT, version TEXT, created_at TEXT, ref02 TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS admin_audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER, action TEXT, details TEXT, created_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS config_history (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT, old_value TEXT, new_value TEXT, changed_by TEXT, created_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS login_history (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, ip_address TEXT, user_agent TEXT, created_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS deposit_history (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, currency TEXT, txn_id TEXT, status TEXT, created_at TEXT)''')
    
    # Revenue Ledger
    c.execute('''CREATE TABLE IF NOT EXISTS revenue_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        user_id INTEGER, 
        amount REAL, 
        description TEXT, 
        type TEXT, 
        created_at TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS user_notifications (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, message TEXT, type TEXT, created_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS server_errors (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, batch_id TEXT, error_msg TEXT, created_at TEXT)''')

    # Defaults
    default_configs = [
        ('slots_monthly_total', '50'), ('slots_monthly_used', '0'),
        ('slots_lifetime_total', '10'), ('slots_lifetime_used', '0'),
        ('system_status', 'OPERATIONAL'), ('worker_paused', '0'),
        ('worker_last_heartbeat', ''), ('archived_revenue', '0.00'),
        ('automation_price_monthly', '29.99'), ('automation_price_lifetime', '499.00')
    ]
    for k, v in default_configs:
        c.execute("INSERT OR IGNORE INTO system_config (key, value) VALUES (?, ?)", (k, v))
        
    conn.commit()
    conn.close()

@login_manager.user_loader
def load_user(user_id):
    from .models import User
    return User.get(user_id)