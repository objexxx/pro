import sqlite3
import uuid
from flask import current_app
from flask_login import UserMixin
from datetime import datetime

# --- UPDATED DB CONNECTION (High Load & Security) ---
def get_db():
    conn = sqlite3.connect(current_app.config['DB_PATH'], timeout=60)
    conn.execute("PRAGMA journal_mode=WAL") # Enable concurrency
    return conn

class User(UserMixin):
    def __init__(self, id, username, email, balance, price_per_label, is_admin, is_banned, api_key, 
                 subscription_end, auto_renew, auth_cookies, auth_csrf, auth_url, auth_file_path, 
                 inventory_json, default_label_type, default_version, default_template, 
                 is_verified=False, otp_code=None, otp_created_at=None):
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
        
        # Defaults
        self.default_label_type = default_label_type or 'priority'
        self.default_version = default_version or '95055'
        self.default_template = default_template or 'pitney_v2'

        # 2FA Fields
        self.is_verified = bool(is_verified)
        self.otp_code = otp_code
        
        # Handle OTP Timestamp
        if otp_created_at and isinstance(otp_created_at, str):
            try:
                self.otp_created_at = datetime.strptime(otp_created_at, "%Y-%m-%d %H:%M:%S")
            except:
                self.otp_created_at = None
        else:
            self.otp_created_at = otp_created_at

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
        try:
            c.execute("""
                SELECT id, username, email, balance, price_per_label, is_admin, is_banned, api_key, 
                       subscription_end, auto_renew, auth_cookies, auth_csrf, auth_url, auth_file_path, 
                       inventory_json, default_label_type, default_version, default_template,
                       is_verified, otp_code, otp_created_at
                FROM users WHERE id = ?
            """, (user_id,))
            data = c.fetchone()
            conn.close()
            if data: return User(*data)
            return None
        except Exception as e:
            print(f"[DB WARN] Schema mismatch in get(): {e}")
            conn.close()
            return None

    @staticmethod
    def get_by_username(username):
        conn = get_db()
        c = conn.cursor()
        try:
            c.execute("""
                SELECT id, username, email, password_hash, balance, price_per_label, is_admin, is_banned, 
                       api_key, subscription_end, auto_renew, auth_cookies, auth_csrf, auth_url, 
                       auth_file_path, inventory_json, default_label_type, default_version, default_template,
                       is_verified, otp_code, otp_created_at
                FROM users WHERE username = ?
            """, (username,))
            data = c.fetchone()
            conn.close()
            return data
        except:
            conn.close()
            return None

    @staticmethod
    def create(username, email, password_hash):
        conn = get_db()
        c = conn.cursor()
        try:
            # --- 1. FETCH SYSTEM PRICES ---
            c.execute("SELECT key, value FROM system_config WHERE key LIKE 'ver_price_%'")
            system_prices = dict(c.fetchall())
            base_price = float(system_prices.get('ver_price_95055', '3.00'))

            new_key = "sk_live_" + str(uuid.uuid4()).replace('-','')[:24]
            now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            created_date = datetime.utcnow().strftime("%Y-%m-%d")

            # --- 2. INSERT USER (UNVERIFIED) ---
            # is_verified = 0 (False) by default
            c.execute("""
                INSERT INTO users (username, email, password_hash, price_per_label, api_key, created_at, is_verified) 
                VALUES (?, ?, ?, ?, ?, ?, 0)
            """, (username, email, password_hash, base_price, new_key, created_date))
            
            user_id = c.lastrowid
            
            # --- 3. SET VERSION PRICES ---
            versions = ['95055', '94888', '94019', '95888', '91149', '93055']
            for ver in versions:
                p = float(system_prices.get(f"ver_price_{ver}", '3.00'))
                c.execute("INSERT INTO user_pricing (user_id, label_type, version, price) VALUES (?, 'priority', ?, ?)", 
                          (user_id, ver, p))

            # --- 4. AUDIT LOG ---
            c.execute("INSERT INTO admin_audit_log (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)", 
                      (0, 'NEW_USER', f"New Registration: {username} ({email})", now_ts))
            
            conn.commit()
            
            # Return new user
            return User.get(user_id)
            
        except Exception as e:
            print(f"User Create Error: {e}")
            return None
        finally: conn.close()

    def update_balance(self, amount):
        """
        ATOMIC BALANCE UPDATE (SECURE)
        Prevents race conditions where users could double-spend balance.
        """
        conn = get_db()
        c = conn.cursor()
        try:
            if amount < 0:
                cost = abs(amount)
                # Ensure balance allows for deduction
                c.execute("UPDATE users SET balance = balance - ? WHERE id = ? AND balance >= ?", (cost, self.id, cost))
            else:
                c.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (amount, self.id))
            
            if c.rowcount > 0:
                conn.commit()
                # Update local object state only if DB update succeeded
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

class SenderAddress:
    def __init__(self, id, user_id, name, company, phone, street1, street2, city, state, zip, phone_alt=None):
        self.id = id
        self.user_id = user_id
        self.name = name
        self.company = company
        self.street1 = street1
        self.street2 = street2
        self.city = city
        self.state = state
        self.zip = zip
        self.phone = phone

    @staticmethod
    def get(id):
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT * FROM sender_addresses WHERE id = ?", (id,))
        row = c.fetchone()
        conn.close()
        if row:
            # Map DB columns to Object (Handle 10 columns)
            # id, user_id, name, company, phone, street1, street2, city, state, zip
            return SenderAddress(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
        return None