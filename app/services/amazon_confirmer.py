import time
import os
import csv
import traceback
import sys
import sqlite3
import requests
import json
import random
import string
import threading
from datetime import datetime

# --- SETUP SYSTEM PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__)) 
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# --- CONFIGURATION ---
DB_PATH = os.path.join(project_root, 'app', 'instance', 'labellab.db')
UPLOADS_FOLDER = os.path.join(project_root, 'data', 'uploads')
BASE_URL = "https://sellercentral.amazon.com/orders-api"

# --- UTILS ---
def parse_cookies_and_csrf(cookie_input):
    cookie_str_parts = []
    extracted_csrf = ""
    raw_list = []
    try:
        if isinstance(cookie_input, list): raw_list = cookie_input
        elif isinstance(cookie_input, str):
            clean = cookie_input.strip()
            if clean.startswith('[') or clean.startswith('{'):
                try: 
                    loaded = json.loads(clean)
                    raw_list = loaded.get('cookies', loaded) if isinstance(loaded, dict) else loaded
                except: pass
        
        if raw_list:
            if isinstance(raw_list, dict): raw_list = [raw_list]
            for item in raw_list:
                if isinstance(item, dict):
                    name, value = item.get('name'), item.get('value')
                    if name and value:
                        cookie_str_parts.append(f"{name}={value}")
                        if name == 'anti-csrftoken-a2z': extracted_csrf = value
                elif isinstance(item, str) and '=' in item:
                    cookie_str_parts.append(item)
                    if 'anti-csrftoken-a2z' in item:
                        try: extracted_csrf = item.split('=')[1].strip().strip(';')
                        except: pass

        if not cookie_str_parts and isinstance(cookie_input, str) and not cookie_input.strip().startswith('{'):
            return cookie_input, "" 
    except Exception as e:
        print(f"[AMAZON BOT] Parsing Warning: {e}")
    return "; ".join(cookie_str_parts), extracted_csrf

def gen_package_id():
    return str(random.randint(100000, 1000000))

def gen_ship_date():
    current_time = time.time()
    seconds_in_a_day = 24 * 60 * 60
    return int(str(int(current_time) - (int(current_time) % seconds_in_a_day)))

# --- DB HELPERS (Thread Safe) ---
def execute_db(query, args=()):
    retries = 3
    while retries > 0:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20) 
            c = conn.cursor()
            c.execute(query, args)
            conn.commit()
            conn.close()
            return
        except sqlite3.OperationalError:
            time.sleep(0.2)
            retries -= 1
        except: return

def get_tracking_from_db(batch_id):
    tracking_map = {}
    print(f"[AMAZON BOT] Fetching tracking from DB for Batch {batch_id}...")
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id, tracking FROM history WHERE batch_id = ? ORDER BY id ASC", (batch_id,))
        for i, row in enumerate(c.fetchall()):
            if row[1] and "FAILED" not in row[1]: tracking_map[str(i + 1)] = row[1]
        print(f"[AMAZON BOT] Loaded {len(tracking_map)} tracking numbers.")
        conn.close()
    except Exception as e: print(f"[AMAZON BOT] DB Error: {e}")
    return tracking_map

def get_file_from_db(batch_id):
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("SELECT filename FROM batches WHERE batch_id = ?", (batch_id,))
        row = c.fetchone(); conn.close()
        if row: return os.path.join(UPLOADS_FOLDER, row[0])
    except: pass
    return None

def detect_delimiter(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            return '\t' if '\t' in f.readline() else ','
    except: return ','

# --- STATUS UPDATERS ---
def update_db_status(order_id, status):
    execute_db("UPDATE history SET status = ? WHERE ref_id = ?", (status, order_id))

def increment_batch_success(batch_id):
    execute_db("UPDATE batches SET success_count = MIN(count, success_count + 1) WHERE batch_id = ?", (batch_id,))

def set_batch_status(batch_id, status):
    execute_db("UPDATE batches SET status = ? WHERE batch_id = ?", (status, batch_id))

def check_already_confirmed(order_id):
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10); c = conn.cursor()
        c.execute("SELECT status FROM history WHERE ref_id = ?", (order_id,))
        row = c.fetchone()
        conn.close()
        if row and row[0] == 'CONFIRMED': return True
        return False
    except: return False

# --- AMAZON API ACTIONS ---
def validate_session(cookies, csrf):
    url = "https://sellercentral.amazon.com/orders-api/order/111-0000000-0000000"
    headers = {
        "accept": "application/json", 
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "cookie": cookies
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        if 'html' in res.headers.get('Content-Type', '').lower() or "<title>Amazon Sign-In</title>" in res.text:
            return False, "SESSION EXPIRED: Redirected to Login Page."
        try: res.json(); return True, "Session Valid"
        except: return False, "INVALID RESPONSE: Could not verify session."
    except Exception as e: return False, f"CONNECTION ERROR: {str(e)}"

def get_shipment_order_info(order_id, cookies, csrf):
    url = f"https://sellercentral.amazon.com/orders-api/order/{order_id}"
    headers = {
        "accept": "application/json",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "cookie": cookies
    }
    try:
        print(f"[DEBUG] [GET] Fetching Info for Order: {order_id}")
        res = requests.get(url, headers=headers, timeout=15)
        if res.status_code != 200: return None, None
        if "<title>Amazon Sign-In</title>" in res.text: return "SESSION_DIED", None
        try: data = res.json()
        except: return "SESSION_DIED", None
        
        address_id = None
        if 'order' in data and 'assignedShipFromLocationAddressId' in data['order']:
            address_id = data['order']['assignedShipFromLocationAddressId']
        
        item_code = None
        if 'order' in data and 'orderItems' in data['order']:
            items = data['order']['orderItems']
            if len(items) > 0: item_code = items[0]['CustomerOrderItemCode']
        
        if address_id and item_code: return item_code, address_id
        else: return None, None
    except Exception as e: return None, None

def confirm_shipment(order_id, tracking_number, item_code, address_id, cookies, csrf):
    url = 'https://sellercentral.amazon.com/orders-api/confirm-shipment'
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "anti-csrftoken-a2z": csrf,
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "cookie": cookies,
        "Referer": f"https://sellercentral.amazon.com/orders-v3/order/{order_id}/confirm-shipment"
    }
    payload = {
        "OrderIdToPackagesList": [{
            "OrderId": order_id,
            "DefaultShippingMethod": None,
            "ConfirmShipmentPackageList": [{
                "packageIdString": gen_package_id(),
                "ItemList": [{ "ItemQty": 1, "CustomerOrderItemCode": item_code }],
                "PackageShippingDetails": {
                    "Carrier": "USPS",
                    "ShipDate": gen_ship_date(), 
                    "ShippingMethod": "Priority Mail",
                    "IsSignatureConfirmationApplied": False,
                    "TrackingId": tracking_number,
                    "ShipFromAddressId": address_id 
                }
            }],
            "ConfirmInvoiceFlag": False
        }],
        "ConfirmInvoiceFlag": False,
        "BulkConfirmShipmentFlag": False
    }
    try:
        print(f"[DEBUG] Sending Confirm Payload for {order_id}...")
        res = requests.post(url, headers=headers, json=payload, timeout=30)
        return res
    except Exception as e: return None

# --- MAIN PROCESS ---
def process_logic(batch_id, txt_path, cookies_input, explicit_csrf):
    print(f"\n[AMAZON BOT] === STARTING BATCH: {batch_id} ===")

    # 1. Update Status to PROCESSING (Triggers UI)
    set_batch_status(batch_id, 'PROCESSING')

    if not txt_path or not os.path.exists(txt_path):
        set_batch_status(batch_id, 'FAILED')
        return False, "Input missing"

    tracking_data = get_tracking_from_db(batch_id)
    if not tracking_data:
        set_batch_status(batch_id, 'FAILED')
        return True, "No labels"

    final_cookie_str, extracted_csrf = parse_cookies_and_csrf(cookies_input)
    final_csrf = extracted_csrf if extracted_csrf else explicit_csrf

    if not final_cookie_str:
        set_batch_status(batch_id, 'FAILED')
        return False, "Invalid Cookies Format"

    delimiter = detect_delimiter(txt_path)
    try:
        with open(txt_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            if reader.fieldnames: reader.fieldnames = [h.strip() for h in reader.fieldnames]
            orders = list(reader)
            
            row_counter = 1
            success_count = 0
            
            for row in orders:
                candidates = [row.get('Ref02'), row.get('Ref01'), row.get('order-id'), row.get('order id')]
                order_id = next((x for x in candidates if x and len(x) > 15), None)
                
                if not order_id: 
                    row_counter += 1; continue

                tracking_number = tracking_data.get(str(row_counter))

                if tracking_number:
                    # --- SMART SKIP (Local DB) ---
                    if check_already_confirmed(order_id):
                        print(f"[AMAZON BOT] [SKIP] {order_id} already confirmed.")
                        increment_batch_success(batch_id) 
                        success_count += 1
                        row_counter += 1
                        continue
                    # --------------------------

                    print(f"\n[AMAZON BOT] Processing Row {row_counter}: {order_id}")
                    
                    # *** FIXED: Added Tracking ID Log ***
                    print(f"[AMAZON BOT] Tracking ID: {tracking_number}") 
                    
                    item_code, address_id = get_shipment_order_info(order_id, final_cookie_str, final_csrf)
                    
                    if item_code == "SESSION_DIED":
                        set_batch_status(batch_id, 'FAILED')
                        return False, "Session Expired Mid-Batch"
                    
                    if not item_code or not address_id:
                        row_counter += 1; continue

                    res = confirm_shipment(order_id, tracking_number, item_code, address_id, final_cookie_str, final_csrf)
                    
                    confirmed = False
                    if res and res.status_code == 200:
                        try:
                            response_text = res.text
                            if 'ConfirmShipmentResponseEnum":"Success"' in response_text or "Success" in response_text:
                                confirmed = True
                            elif 'ConfirmShipmentResponseEnum":"AlreadyShipped"' in response_text or "AlreadyShipped" in response_text:
                                confirmed = True
                        except: pass
                    elif res and "already confirmed" in res.text.lower():
                         confirmed = True

                    if confirmed:
                        update_db_status(order_id, 'CONFIRMED')
                        increment_batch_success(batch_id)
                        success_count += 1

                row_counter += 1
                time.sleep(0.5)

        print(f"[AMAZON BOT] FINISHED BATCH {batch_id}. Confirmed: {success_count}")
        
        # 2. Update Status Back to COMPLETED (Triggers UI Success Popup)
        set_batch_status(batch_id, 'COMPLETED')
        
        return True, "Batch Processed"

    except Exception as e:
        print(f"[AMAZON BOT] [CRASH] {e}")
        traceback.print_exc()
        set_batch_status(batch_id, 'FAILED')
        return False, str(e)

# --- ENTRY POINT (THREADED) ---
def run_thread(batch_id, cookies, csrf):
    target_txt_path = get_file_from_db(batch_id)
    if target_txt_path:
        process_logic(batch_id, target_txt_path, cookies, csrf)

def run_confirmation(batch_id=None, cookies=None, csrf=None, *args, **kwargs):
    # Spawns background thread so server doesn't freeze
    t = threading.Thread(target=run_thread, args=(batch_id, cookies, csrf))
    t.start()
    return True, "Background Process Started"

if __name__ == "__main__":
    pass