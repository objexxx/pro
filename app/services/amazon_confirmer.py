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
BASE_URL = "https://sellercentral.amazon.com/orders-api"

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

def get_shipment_order_info(order_id, cookies, csrf):
    url = f"https://sellercentral.amazon.com/orders-api/order/{order_id}"
    params = {"ts": time.time()} # Cache Buster
    headers = {
        "accept": "application/json",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "cookie": cookies
    }
    try:
        print(f"[DEBUG] [GET] Fetching Info for Order: {order_id}")
        res = requests.get(url, headers=headers, params=params, timeout=15)
        
        if res.status_code != 200: return None, None, []
        if "<title>Amazon Sign-In</title>" in res.text: return "SESSION_DIED", None, []
        try: data = res.json()
        except: return "SESSION_DIED", None, []
        
        address_id = None
        if 'order' in data and 'assignedShipFromLocationAddressId' in data['order']:
            address_id = data['order']['assignedShipFromLocationAddressId']
        
        item_code = None
        if 'order' in data and 'orderItems' in data['order']:
            items = data['order']['orderItems']
            if len(items) > 0: item_code = items[0]['CustomerOrderItemCode']
            
        existing_tracking_ids = []
        if 'order' in data and 'packages' in data['order']:
            packages = data['order']['packages']
            if packages:
                for pkg in packages:
                    if 'trackingId' in pkg and pkg['trackingId']:
                        existing_tracking_ids.append(str(pkg['trackingId']).strip())
        
        return item_code, address_id, existing_tracking_ids
    except Exception as e: return None, None, []

def confirm_shipment_single(order_id, tracking_number, item_code, address_id, cookies, csrf):
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
        print(f"[DEBUG] Confirming {order_id} -> {tracking_number}")
        res = requests.post(url, headers=headers, json=payload, timeout=30)
        return res
    except Exception as e: return None

# --- MAIN PROCESS ---
def process_logic(batch_id, txt_path, cookies_input, explicit_csrf):
    print(f"\n[AMAZON BOT] === STARTING BATCH: {batch_id} ===")

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
            
            # --- STEP 1: GROUP ROWS ---
            grouped_orders = defaultdict(list)
            original_row_index = 1 
            
            for row in reader:
                candidates = [row.get('Ref02'), row.get('Ref01'), row.get('order-id'), row.get('order id')]
                order_id = next((x for x in candidates if x and len(x) > 15), None)
                if order_id: grouped_orders[order_id].append(original_row_index)
                original_row_index += 1
            
            # --- STEP 2: LOAD LOCAL HISTORY ---
            local_history = load_history()
            
            success_count = 0
            
            for order_id, row_indices in grouped_orders.items():
                
                # Get Set of CSV Tracking Numbers (Unique for this order)
                csv_tracking_set = set()
                for idx in row_indices:
                    tn = tracking_data.get(str(idx))
                    if tn: csv_tracking_set.add(tn.strip())
                
                if not csv_tracking_set: continue 

                print(f"\n[AMAZON BOT] Checking Order: {order_id}")
                
                # Fetch Amazon Info
                item_code, address_id, amazon_tracking_list = get_shipment_order_info(order_id, final_cookie_str, final_csrf)
                amazon_tracking_set = set(amazon_tracking_list)

                if item_code == "SESSION_DIED":
                    set_batch_status(batch_id, 'FAILED')
                    return False, "Session Expired Mid-Batch"
                
                if not item_code or not address_id:
                    print(f"[AMAZON BOT] Failed to get Order Info for {order_id}")
                    # DO NOT increment success_count if we failed to get order info
                    continue

                # --- TRIPLE CHECK LOGIC ---
                # 1. Is it on Amazon?
                # 2. Is it in our Local History (sent previously)?
                to_upload = []
                for tn in csv_tracking_set:
                    if tn in amazon_tracking_set:
                        # Amazon already has it
                        continue
                    if tn in local_history:
                        print(f"[AMAZON BOT] SKIP: {tn} found in LOCAL history (already sent).")
                        continue
                    
                    to_upload.append(tn)

                print(f"[AMAZON BOT] > Found on Amazon: {len(amazon_tracking_set)}")
                print(f"[AMAZON BOT] > Needs Upload: {len(to_upload)}")

                if not to_upload:
                    increment_batch_success(batch_id, len(row_indices))
                    success_count += len(row_indices)
                    continue

                confirmed_in_group = 0
                
                for tn in to_upload:
                    res = confirm_shipment_single(order_id, tn, item_code, address_id, final_cookie_str, final_csrf)
                    
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
                        confirmed_in_group += 1
                        save_to_history(tn) 
                        print(f"[AMAZON BOT] -> SUCCESS: Added {tn}")
                        time.sleep(2.0) 
                    else:
                        print(f"[AMAZON BOT] FAILED to confirm Tracking {tn}")

                # Mark success if we handled the uploads
                update_db_status(order_id, 'CONFIRMED')
                increment_batch_success(batch_id, len(row_indices)) 
                success_count += len(row_indices)

                time.sleep(0.5)

        print(f"[AMAZON BOT] FINISHED BATCH {batch_id}. Total Confirmed Rows: {success_count}")
        
        # --- FIXED: Only set to CONFIRMED if we actually succeeded ---
        # This prevents locking the button on total failure
        if success_count > 0:
            set_batch_status(batch_id, 'CONFIRMED')
        else:
            print("[AMAZON BOT] Batch Failed (0 success). Setting status to FAILED.")
            set_batch_status(batch_id, 'FAILED')
        
        return True, "Batch Processed"

    except Exception as e:
        print(f"[AMAZON BOT] [CRASH] {e}")
        traceback.print_exc()
        set_batch_status(batch_id, 'FAILED')
        return False, str(e)

# --- ENTRY POINT (DIRECT CALL - NO INNER THREAD) ---
def run_thread(batch_id, cookies, csrf):
    target_txt_path = get_file_from_db(batch_id)
    if target_txt_path:
        process_logic(batch_id, target_txt_path, cookies, csrf)

def run_confirmation(batch_id=None, cookies=None, csrf=None, *args, **kwargs):
    # FIXED: Run directly since routes.py already handles the threading
    run_thread(batch_id, cookies, csrf)
    return True, "Process Completed"

if __name__ == "__main__":
    pass