import sqlite3
import uuid
from flask import current_app
from flask_login import UserMixin
from datetime import datetime

def get_db():
    return sqlite3.connect(current_app.config['DB_PATH'], timeout=30)

class User(UserMixin):
    def __init__(self, id, username, email, balance, price_per_label, is_admin, is_banned, api_key, 
                 subscription_end, auto_renew, auth_cookies, auth_csrf, auth_url, auth_file_path, 
                 inventory_json, default_label_type, default_version, default_template):
        self.id = id
        self.username = username
        self.email = email
        self.balance = balance
        self.price_per_label = price_per_label
        self.is_admin = bool(is_admin)
        self.is_banned = bool(is_banned)
        self.api_key = api_key
        self.subscription_end = subscription_end
        self.auto_renew = bool(auto_renew)
        self.auth_cookies = auth_cookies
        self.auth_csrf = auth_csrf
        self.auth_url = auth_url
        self.auth_file_path = auth_file_path
        self.inventory_json = inventory_json
        
        # New Defaults
        self.default_label_type = default_label_type or 'priority'
        self.default_version = default_version or '95055'
        self.default_template = default_template or 'pitney_v2'

    @property
    def is_subscribed(self):
        if not self.subscription_end: return False
        try:
            end_date = datetime.strptime(self.subscription_end, "%Y-%m-%d %H:%M:%S")
            return end_date > datetime.utcnow()
        except: return False

    @staticmethod
    def get(user_id):
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            SELECT id, username, email, balance, price_per_label, is_admin, is_banned, api_key, 
                   subscription_end, auto_renew, auth_cookies, auth_csrf, auth_url, auth_file_path, 
                   inventory_json, default_label_type, default_version, default_template 
            FROM users WHERE id = ?
        """, (user_id,))
        data = c.fetchone()
        conn.close()
        if data: return User(*data)
        return None

    @staticmethod
    def get_by_username(username):
        conn = get_db()
        c = conn.cursor()
        # Note: This query returns raw tuple including password_hash, which is handled in login route
        # We don't use the User class constructor here directly in login route logic usually
        # But for consistency in models, we typically only use this for checking auth
        c.execute("""
            SELECT id, username, email, password_hash, balance, price_per_label, is_admin, is_banned, 
                   api_key, subscription_end, auto_renew, auth_cookies, auth_csrf, auth_url, 
                   auth_file_path, inventory_json, default_label_type, default_version, default_template 
            FROM users WHERE username = ?
        """, (username,))
        data = c.fetchone()
        conn.close()
        return data

    @staticmethod
    def create(username, email, password_hash):
        conn = get_db()
        c = conn.cursor()
        try:
            new_key = "sk_live_" + str(uuid.uuid4()).replace('-','')[:24]
            now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            created_date = datetime.utcnow().strftime("%Y-%m-%d")

            # 1. Insert User
            c.execute("INSERT INTO users (username, email, password_hash, price_per_label, api_key, created_at) VALUES (?, ?, ?, ?, ?, ?)", 
                      (username, email, password_hash, 3.00, new_key, created_date))
            
            # 2. Add to Admin Audit Log (admin_id 0 represents 'SYSTEM')
            c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", 
                      (0, 'NEW_USER', f"New Registration: {username} ({email})", now_ts))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"User Create Error: {e}")
            return False
        finally: conn.close()

    def update_balance(self, amount):
        conn = get_db()
        c = conn.cursor()
        try:
            if amount < 0:
                cost = abs(amount)
                c.execute("UPDATE users SET balance = balance - ? WHERE id = ? AND balance >= ?", (cost, self.id, cost))
            else:
                c.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (amount, self.id))
            
            if c.rowcount > 0:
                conn.commit()
                self.balance += amount 
                conn.close()
                return True
            else:
                conn.close()
                return False
        except:
            conn.close()
            return False
        
    def activate_subscription(self, days=30, auto_renew=False):
        conn = get_db()
        c = conn.cursor()
        from datetime import timedelta
        new_end = (datetime.utcnow() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE users SET subscription_end = ?, auto_renew = ? WHERE id = ?", (new_end, int(auto_renew), self.id))
        conn.commit()
        conn.close()

    def update_settings(self, cookies, csrf, filename, inventory, auto_renew):
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE users SET auth_cookies = ?, auth_csrf = ?, auth_file_path = ?, inventory_json = ?, auto_renew = ? WHERE id = ?", 
                  (cookies, csrf, filename, inventory, int(auto_renew), self.id))
        conn.commit()
        conn.close()
        
    def update_defaults(self, l_type, ver, tmpl):
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE users SET default_label_type = ?, default_version = ?, default_template = ? WHERE id = ?", 
                  (l_type, ver, tmpl, self.id))
        conn.commit()
        conn.close()