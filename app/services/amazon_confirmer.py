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
import re
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
    return str(random.randint(100000000, 999999999))

def gen_ship_date():
    current_time = time.time()
    seconds_in_a_day = 24 * 60 * 60
    return int(str(int(current_time) - (int(current_time) % seconds_in_a_day)))

def is_amazon_order_id(val):
    if not val: return False
    return bool(re.search(r'\d{3}-\d{7}-\d{7}', str(val)))

# --- DB HELPERS ---
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

def get_tracking_map_by_order_id(batch_id):
    tracking_map = defaultdict(list)
    print(f"[AMAZON BOT] Fetching tracking from DB for Batch {batch_id}...")
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT ref_id, ref02, tracking FROM history WHERE batch_id = ?", (batch_id,))
        rows = c.fetchall()
        
        valid_count = 0
        swapped_count = 0
        
        for row in rows:
            col_ref_id = str(row[0]).strip()
            col_ref02 = str(row[1]).strip()
            tn = str(row[2]).strip()
            
            if not tn or "FAILED" in tn: continue

            final_order_id = None
            if is_amazon_order_id(col_ref02):
                final_order_id = col_ref02
                valid_count += 1
            elif is_amazon_order_id(col_ref_id):
                final_order_id = col_ref_id
                swapped_count += 1
            
            if final_order_id:
                tracking_map[final_order_id].append(tn)
        
        print(f"[AMAZON BOT] Loaded tracking for {len(tracking_map)} unique orders.")
        if swapped_count > 0:
            print(f"[DEBUG] DETECTED SWAPPED DATABASE COLUMNS ({swapped_count} rows). Auto-Corrected.")
        
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

def update_db_status(batch_id, order_id, status):
    execute_db(
        "UPDATE history SET status = ? WHERE batch_id = ? AND (ref_id = ? OR ref02 = ?)",
        (status, batch_id, order_id, order_id)
    )

def update_tracking_status(batch_id, tracking, status):
    if not tracking:
        return
    execute_db(
        "UPDATE history SET status = ? WHERE batch_id = ? AND tracking = ?",
        (status, batch_id, tracking)
    )

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
    
    # --- FIX: REMOVED reset of success_count. Preserves label generation count. ---
    set_batch_status(batch_id, 'CONFIRMING')

    if not txt_path or not os.path.exists(txt_path):
        set_batch_status(batch_id, 'CONFIRM_FAILED')
        return False, "Input missing"

    tracking_map = get_tracking_map_by_order_id(batch_id)
    if not tracking_map:
        print("[AMAZON BOT] DB returned empty map. No valid Order IDs found.")
        # Only set failed if we genuinely found nothing (meaning initial gen likely failed too)
        # But usually we want to keep it as is if labels exist.
        set_batch_status(batch_id, 'CONFIRM_FAILED') 
        return True, "No labels"

    final_cookie_str, extracted_csrf = parse_cookies_and_csrf(cookies_input)
    final_csrf = extracted_csrf if extracted_csrf else explicit_csrf

    if not final_cookie_str:
        set_batch_status(batch_id, 'CONFIRM_FAILED')
        return False, "Invalid Cookies Format"

    session = requests.Session()
    session.headers.update({
        "accept": "application/json",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "cookie": final_cookie_str,
        "anti-csrftoken-a2z": final_csrf
    })

    delimiter = detect_delimiter(txt_path)
    attempts = 0
    failures = 0
    FAIL_FAST_LIMIT = 10 
    has_successful_fetch = False 
    
    try:
        with open(txt_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            if reader.fieldnames: reader.fieldnames = [h.strip() for h in reader.fieldnames]
            
            unique_orders = set()
            for row in reader:
                candidates = [row.get('Ref02'), row.get('Ref01'), row.get('order-id'), row.get('order id')]
                order_id = next((x for x in candidates if x and len(x) > 15), None)
                if order_id: unique_orders.add(order_id)
            
            local_history = load_history()
            
            for order_id in unique_orders:
                
                db_tracking_list = tracking_map.get(order_id, [])
                
                print(f"\n[AMAZON BOT] Checking Order: {order_id}")
                print(f" > Found {len(db_tracking_list)} tracking numbers in DB.")

                if not db_tracking_list: 
                    print(" > No tracking found for this order. Skipping.")
                    continue 

                attempts += 1
                fetch_url = f"https://sellercentral.amazon.com/orders-api/order/{order_id}"
                address_id = None
                
                existing_package_ids = [] 
                needed_items_queue = [] 
                
                try:
                    res = session.get(fetch_url, params={"ts": time.time()}, timeout=15)
                    if res.status_code == 200 and "<title>Amazon Sign-In</title>" not in res.text:
                        data = res.json()
                        if 'order' in data:
                            if 'assignedShipFromLocationAddressId' in data['order']:
                                address_id = data['order']['assignedShipFromLocationAddressId']
                            
                            if 'orderItems' in data['order']:
                                for item in data['order']['orderItems']:
                                    code = item.get('CustomerOrderItemCode')
                                    try:
                                        qty_ordered = int(item.get('QuantityOrdered', 0))
                                        qty_shipped = int(item.get('QuantityShipped', 0))
                                        qty_needed = qty_ordered - qty_shipped
                                        
                                        # If fully shipped, force it to 1 so we can EDIT it
                                        if qty_needed <= 0: qty_needed = 1 
                                        
                                        for _ in range(qty_needed):
                                            needed_items_queue.append(code)
                                    except: pass

                            # --- CRITICAL FIX: CHECK FOR "PackageId" (Capitalized) ---
                            if 'packages' in data['order'] and data['order']['packages']:
                                for pkg in data['order']['packages']:
                                    # We check PackageId (Standard), packageId (Legacy), id (Fallback)
                                    pid = pkg.get('PackageId') or pkg.get('packageId') or pkg.get('packageIdString') or pkg.get('id')
                                    if pid:
                                        existing_package_ids.append(str(pid))

                    elif "<title>Amazon Sign-In</title>" in res.text:
                        needed_items_queue = ["SESSION_DIED"] 
                except: pass

                is_dead = (len(needed_items_queue) == 1 and needed_items_queue[0] == "SESSION_DIED")
                if not is_dead and address_id: has_successful_fetch = True 
                
                if not address_id and not has_successful_fetch and attempts <= FAIL_FAST_LIMIT:
                    failures += 1
                    if failures >= FAIL_FAST_LIMIT:
                        print("[AMAZON BOT] ABORTING: Too many initial failures.")
                        # --- FIX: Do NOT reset success_count here ---
                        set_batch_status(batch_id, 'AUTH_ERROR')
                        return False, "Auth Error"

                if is_dead:
                    # --- FIX: Do NOT reset success_count here ---
                    set_batch_status(batch_id, 'AUTH_ERROR')
                    return False, "Session Expired Mid-Batch"
                
                if not address_id:
                    print(f"[AMAZON BOT] Failed to get Order Info/Address. Skipping.")
                    continue

                to_upload = db_tracking_list
                group_success = True
                confirm_url = 'https://sellercentral.amazon.com/orders-api/confirm-shipment'
                
                for i, tn in enumerate(to_upload):
                    target_item_code = None
                    if i < len(needed_items_queue):
                        target_item_code = needed_items_queue[i]
                    else:
                        if needed_items_queue: target_item_code = needed_items_queue[0]
                        elif data.get('order', {}).get('orderItems'): 
                            target_item_code = data['order']['orderItems'][0].get('CustomerOrderItemCode')

                    if not target_item_code:
                        print(f"[AMAZON BOT] No valid Item Code found for {tn}. Skipping.")
                        continue

                    # --- OVERWRITE LOGIC ---
                    is_edit = False
                    if existing_package_ids:
                        pkg_id = existing_package_ids.pop(0) # Reuse ID
                        is_edit = True
                        print(f"[DEBUG] Found ID {pkg_id}. Sending EDIT update.")
                    else:
                        pkg_id = gen_package_id() # New Package
                        print("[DEBUG] No Existing ID found. Sending NEW shipment.")
                    
                    payload = {
                        "OrderIdToPackagesList": [{
                            "OrderId": order_id,
                            "DefaultShippingMethod": None,
                            "ConfirmShipmentPackageList": [{
                                "packageIdString": pkg_id,
                                "ItemList": [{ "ItemQty": 1, "CustomerOrderItemCode": target_item_code }], 
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
                        "EditShipmentFlag": is_edit,
                        "BulkConfirmShipmentFlag": False
                    }
                    
                    session.headers.update({"Referer": f"https://sellercentral.amazon.com/orders-v3/order/{order_id}/confirm-shipment"})
                    
                    confirmed = False
                    try:
                        print(f"[DEBUG] POSTing {tn} to Package {pkg_id}")
                        res = session.post(confirm_url, json=payload, timeout=30)
                        
                        if res.status_code == 200:
                            if 'ConfirmShipmentResponseEnum":"Success"' in res.text or "Success" in res.text: 
                                confirmed = True
                                print(f"[AMAZON BOT] -> SUCCESS: Added/Updated {tn}")
                            elif 'ConfirmShipmentResponseEnum":"AlreadyShipped"' in res.text:
                                confirmed = True
                                print(f"[AMAZON BOT] -> ALREADY SHIPPED (Amazon rejected edit): {tn}")
                            else:
                                print(f"[AMAZON BOT] -> UNKNOWN RESPONSE: {res.text[:100]}...")
                        else:
                            print(f"[AMAZON BOT] -> FAILED ({res.status_code}): {res.text[:100]}")
                    except Exception as e:
                        print(f"[AMAZON BOT] -> EXCEPTION: {e}")
                    
                    if confirmed:
                        save_to_history(tn)
                        update_tracking_status(batch_id, tn, 'CONFIRMED')
                    else:
                        group_success = False

                    time.sleep(random.uniform(0.8, 1.2))

                if group_success: update_db_status(batch_id, order_id, 'CONFIRMED')
                # --- FIX: Removed increment_batch_success to prevent double counting ---
                time.sleep(0.1)

        print(f"[AMAZON BOT] FINISHED BATCH {batch_id}.")
        set_batch_status(batch_id, 'CONFIRMED')
        return True, "Batch Processed"

    except Exception as e:
        print(f"[AMAZON BOT] [CRASH] {e}")
        traceback.print_exc()
        set_batch_status(batch_id, 'CONFIRM_FAILED')
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
