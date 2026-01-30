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
from collections import defaultdict
from datetime import datetime

# --- SETUP SYSTEM PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__)) 
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# --- CONFIGURATION ---
DB_PATH = os.path.join(project_root, 'app', 'instance', 'labellab.db')
UPLOADS_FOLDER = os.path.join(project_root, 'data', 'uploads')
HISTORY_FILE = os.path.join(project_root, 'data', 'sent_tracking_history.json')

# --- HISTORY MANAGER ---
def load_history():
    if not os.path.exists(HISTORY_FILE): return set()
    try:
        with open(HISTORY_FILE, 'r') as f:
            return set(json.load(f))
    except: return set()

def save_to_history(tracking_number):
    try:
        history = load_history()
        history.add(str(tracking_number).strip())
        with open(HISTORY_FILE, 'w') as f:
            json.dump(list(history), f)
    except: pass

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
            if row[1] and "FAILED" not in row[1]: 
                tracking_map[str(i + 1)] = str(row[1]).strip()
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

def increment_batch_success(batch_id, count=1):
    for _ in range(count):
        execute_db("UPDATE batches SET success_count = MIN(count, success_count + 1) WHERE batch_id = ?", (batch_id,))

def set_batch_status(batch_id, status):
    execute_db("UPDATE batches SET status = ? WHERE batch_id = ?", (status, batch_id))

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

# --- MAIN PROCESS ---
def process_logic(batch_id, txt_path, cookies_input, explicit_csrf):
    print(f"\n[AMAZON BOT] === STARTING BATCH: {batch_id} ===")
    
    # Init Count to 0 for progress bar
    execute_db("UPDATE batches SET success_count = 0 WHERE batch_id = ?", (batch_id,))
    set_batch_status(batch_id, 'CONFIRMING')

    if not txt_path or not os.path.exists(txt_path):
        set_batch_status(batch_id, 'CONFIRM_FAILED')
        return False, "Input missing"

    tracking_data = get_tracking_from_db(batch_id)
    if not tracking_data:
        set_batch_status(batch_id, 'CONFIRM_FAILED')
        return True, "No labels"

    final_cookie_str, extracted_csrf = parse_cookies_and_csrf(cookies_input)
    final_csrf = extracted_csrf if extracted_csrf else explicit_csrf

    if not final_cookie_str:
        set_batch_status(batch_id, 'CONFIRM_FAILED')
        return False, "Invalid Cookies Format"

    # === PERSISTENT SESSION ===
    session = requests.Session()
    session.headers.update({
        "accept": "application/json",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "cookie": final_cookie_str,
        "anti-csrftoken-a2z": final_csrf
    })

    delimiter = detect_delimiter(txt_path)
    total_confirmed_rows = 0
    
    # --- FAIL FAST SETUP ---
    attempts = 0
    failures = 0
    FAIL_FAST_LIMIT = 10 
    has_successful_fetch = False 
    
    try:
        with open(txt_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            if reader.fieldnames: reader.fieldnames = [h.strip() for h in reader.fieldnames]
            
            grouped_orders = defaultdict(list)
            original_row_index = 1 
            for row in reader:
                candidates = [row.get('Ref02'), row.get('Ref01'), row.get('order-id'), row.get('order id')]
                order_id = next((x for x in candidates if x and len(x) > 15), None)
                if order_id: grouped_orders[order_id].append(original_row_index)
                original_row_index += 1
            
            local_history = load_history()
            
            for order_id, row_indices in grouped_orders.items():
                
                csv_tracking_set = set()
                for idx in row_indices:
                    tn = tracking_data.get(str(idx))
                    if tn: csv_tracking_set.add(tn.strip())
                
                if not csv_tracking_set: continue 

                print(f"\n[AMAZON BOT] Checking Order: {order_id}")
                
                # --- FETCH INFO ---
                attempts += 1
                
                fetch_url = f"https://sellercentral.amazon.com/orders-api/order/{order_id}"
                item_code = None
                address_id = None
                amazon_tracking_list = []
                
                try:
                    res = session.get(fetch_url, params={"ts": time.time()}, timeout=15)
                    
                    if res.status_code == 200 and "<title>Amazon Sign-In</title>" not in res.text:
                        data = res.json()
                        if 'order' in data:
                            if 'assignedShipFromLocationAddressId' in data['order']:
                                address_id = data['order']['assignedShipFromLocationAddressId']
                            if 'orderItems' in data['order'] and len(data['order']['orderItems']) > 0:
                                item_code = data['order']['orderItems'][0]['CustomerOrderItemCode']
                            if 'packages' in data['order'] and data['order']['packages']:
                                for pkg in data['order']['packages']:
                                    if 'trackingId' in pkg and pkg['trackingId']:
                                        amazon_tracking_list.append(str(pkg['trackingId']).strip())
                    elif "<title>Amazon Sign-In</title>" in res.text:
                        item_code = "SESSION_DIED"
                except: pass

                # --- FAIL FAST LOGIC ---
                if item_code and item_code != "SESSION_DIED":
                    has_successful_fetch = True 
                
                if not item_code and not has_successful_fetch and attempts <= FAIL_FAST_LIMIT:
                    failures += 1
                    print(f"[AMAZON BOT] Fail Fast Check: {failures}/{attempts} failed.")
                    if failures >= FAIL_FAST_LIMIT:
                        print("[AMAZON BOT] ABORTING: Too many initial failures.")
                        execute_db("UPDATE batches SET success_count = 0 WHERE batch_id = ?", (batch_id,))
                        set_batch_status(batch_id, 'AUTH_ERROR')
                        return False, "Auth Error"

                if item_code == "SESSION_DIED":
                    execute_db("UPDATE batches SET success_count = 0 WHERE batch_id = ?", (batch_id,))
                    set_batch_status(batch_id, 'AUTH_ERROR')
                    return False, "Session Expired Mid-Batch"
                
                if not item_code or not address_id:
                    print(f"[AMAZON BOT] Failed to get Order Info. Skipping.")
                    increment_batch_success(batch_id, len(row_indices))
                    total_confirmed_rows += len(row_indices)
                    continue

                amazon_tracking_set = set(amazon_tracking_list)
                to_upload = []
                for tn in csv_tracking_set:
                    if tn in amazon_tracking_set: continue
                    if tn in local_history: continue
                    to_upload.append(tn)

                if not to_upload:
                    increment_batch_success(batch_id, len(row_indices))
                    total_confirmed_rows += len(row_indices)
                    continue

                group_success = True
                confirm_url = 'https://sellercentral.amazon.com/orders-api/confirm-shipment'
                
                for tn in to_upload:
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
                                    "TrackingId": tn,
                                    "ShipFromAddressId": address_id 
                                }
                            }],
                            "ConfirmInvoiceFlag": False
                        }],
                        "ConfirmInvoiceFlag": False,
                        "BulkConfirmShipmentFlag": False
                    }
                    
                    session.headers.update({"Referer": f"https://sellercentral.amazon.com/orders-v3/order/{order_id}/confirm-shipment"})
                    
                    confirmed = False
                    try:
                        print(f"[DEBUG] Confirming {order_id} -> {tn}")
                        res = session.post(confirm_url, json=payload, timeout=30)
                        if res.status_code == 200:
                            if 'ConfirmShipmentResponseEnum":"Success"' in res.text or "Success" in res.text: confirmed = True
                            elif 'ConfirmShipmentResponseEnum":"AlreadyShipped"' in res.text: confirmed = True
                    except: pass
                    
                    if confirmed:
                        save_to_history(tn) 
                        print(f"[AMAZON BOT] -> SUCCESS: Added {tn}")
                    else:
                        print(f"[AMAZON BOT] FAILED to confirm Tracking {tn}")
                        group_success = False

                    # --- OPTIMIZED SPEED: Random jitter 0.5s - 1.0s ---
                    time.sleep(random.uniform(0.5, 1.0))

                if group_success:
                    update_db_status(order_id, 'CONFIRMED')
                
                increment_batch_success(batch_id, len(row_indices))
                total_confirmed_rows += len(row_indices)
                
                time.sleep(0.1)

        print(f"[AMAZON BOT] FINISHED BATCH {batch_id}. Total Confirmed: {total_confirmed_rows}")
        
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("SELECT count FROM batches WHERE batch_id = ?", (batch_id,))
        total_rows = c.fetchone()[0]
        conn.close()

        if total_confirmed_rows >= total_rows:
            set_batch_status(batch_id, 'CONFIRMED')
        elif total_confirmed_rows > 0:
            set_batch_status(batch_id, 'PARTIAL')
        else:
            set_batch_status(batch_id, 'CONFIRM_FAILED')
        
        return True, "Batch Processed"

    except Exception as e:
        print(f"[AMAZON BOT] [CRASH] {e}")
        traceback.print_exc()
        set_batch_status(batch_id, 'CONFIRM_FAILED')
        # --- SECURITY FIX: HIDE INTERNAL ERRORS FROM USER ---
        return False, "Processing Error (Contact Support)"

def run_thread(batch_id, cookies, csrf):
    target_txt_path = get_file_from_db(batch_id)
    if target_txt_path:
        process_logic(batch_id, target_txt_path, cookies, csrf)

def run_confirmation(batch_id=None, cookies=None, csrf=None, *args, **kwargs):
    run_thread(batch_id, cookies, csrf)
    return True, "Process Completed"

if __name__ == "__main__":
    pass