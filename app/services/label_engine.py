import random
import requests
import io
import os
import json
import time
import sqlite3
import re
from datetime import datetime, timedelta
from pypdf import PdfWriter
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

class LabelEngine:
    def __init__(self):
        self.mode = "production"
        self.debug_mode = True 
        self.sanitize_zpl = lambda text: str(text).replace("^", "").replace("~", "") if text else ""

    # --- HELPERS ---
    def get_mailer_id(self, version, data_folder):
        json_path = os.path.join(data_folder, 'mailer_ids.json')
        default_ids = ['90000000'] 
        
        if not os.path.exists(json_path): return random.choice(default_ids)
        try:
            with open(json_path, 'r') as f: data = json.load(f)
            ver_str = str(version).strip()
            if "94888" in ver_str: mids = data.get('ids_94888', [])
            else: mids = data.get('ids_95055', [])
            return str(random.choice(mids or default_ids)).strip()
        except: return random.choice(default_ids)

    def calculate_usps_check_digit(self, body):
        s = str(body)
        total = sum(int(c) for i, c in enumerate(s) if i % 2 == 0) * 3 + sum(int(c) for i, c in enumerate(s) if i % 2 != 0)
        return "0" if total % 10 == 0 else str(10 - (total % 10))

    def generate_unique_tracking(self, version, mailer_id, seq_code=None):
        ver_str = str(version).strip()
        
        if "94888" in ver_str:
            stc = "94888"
            if len(mailer_id) == 9: serial = f"{random.randint(1000000, 9999999)}"
            else: serial = f"{random.randint(10000000, 99999999)}"
            body = f"{stc}{mailer_id}{serial}"
            return body + self.calculate_usps_check_digit(body)
        else:
            stc = "9505"
            day_code = seq_code if seq_code else datetime.now().strftime('%j')
            rand_part = f"{random.randint(0, 99999):05d}"
            serial = f"{day_code}{rand_part}"
            body = f"{stc}{mailer_id}{serial}"
            return body + self.calculate_usps_check_digit(body)

    def get_region(self, state):
        state = str(state).strip().upper()
        regions = {'West': ['CA','OR','WA','NV','AZ','ID','UT','HI','AK'], 'Mountain': ['MT','WY','CO','NM','ND','SD','NE','KS','OK'], 'Midwest': ['MN','IA','MO','WI','IL','MI','IN','OH','KY'], 'South': ['TX','AR','LA','MS','AL','TN','GA','FL','SC','NC','VA','WV'], 'Northeast': ['PA','NY','VT','NH','ME','MA','RI','CT','NJ','DE','MD','DC']}
        for r, s in regions.items(): 
            if state in s: return r
        return 'Midwest'

    def calculate_zone(self, state_from, state_to):
        r1, r2 = self.get_region(state_from), self.get_region(state_to)
        if r1 == r2: return "2" if str(state_from).upper() == str(state_to).upper() else "3"
        reg_map = {'West': 0, 'Mountain': 1, 'Midwest': 2, 'South': 3, 'Northeast': 4}
        dist = abs(reg_map[r1] - reg_map[r2])
        return str(dist + 4) if dist < 4 else "8"

    def calculate_transit_days(self, zone): z = int(zone); return 1 if z<=2 else 2 if z==3 else 3 if z<=5 else 4
    def generate_carrier_route(self, zip_code): return f"C{random.randint(1,99):03d}"
    
    # --- ADDRESS FORMATTER ---
    def format_address(self, name, company, street, street2, city, state, zip_val):
        parts = []
        if name and str(name).lower()!='nan': parts.append(self.sanitize_zpl(str(name).strip()))
        if company and str(company).lower()!='nan': parts.append(self.sanitize_zpl(str(company).strip()))
        if street and str(street).lower()!='nan': parts.append(self.sanitize_zpl(str(street).strip()))
        if street2 and str(street2).lower()!='nan': parts.append(self.sanitize_zpl(str(street2).strip()))
        parts.append(f"{self.sanitize_zpl(str(city))} {self.sanitize_zpl(str(state))} {self.sanitize_zpl(str(zip_val))}".strip())
        return "\\&".join(parts)

    def generate_0901_number(self): return f"090100000{random.randint(1000, 9999)}"
    def generate_random_account_info(self): return f"028W{random.randint(1000000000, 9999999999)}", str(random.randint(3000000000, 3999999999)) 
    def generate_c_number(self): return f"C{random.randint(1000000, 9999999)}"
    def generate_stamps_refs(self): return f"063S00000{random.randint(10000, 99999)}", f"{random.randint(1000000, 9999999)}"

    # --- PROCESSOR ---
    def process_single_label(self, row, version, templates, template_choice, batch_seq_code, now, today, data_folder):
        # 1. INITIALIZE VARIABLES SAFELY (Prevents 'Not Defined' Crashes)
        w_text = "1.0 LB" 
        weight_val = 1.0
        raw_w = "1"
        order_id = "UNKNOWN"
        t_choice = str(template_choice).lower()
        
        def safe_get(key, default=""):
            val = row.get(key)
            if pd.isna(val) or str(val).lower() == 'nan': return default
            return self.sanitize_zpl(str(val).strip())

        try:
            # --- INPUT PROCESSING (FORCED CAPITALIZATION) ---
            to_z = safe_get('ZipTo')
            if not to_z or len(to_z) < 5: 
                print(f" > Skipped Row: Invalid Zip '{to_z}'")
                return None, None

            # Force Uppercase on EVERYTHING
            order_id = safe_get('Ref01').upper()
            sku = safe_get('Ref02').upper()
            desc_val = safe_get('Description').upper()
            
            from_n = safe_get('FromName').upper()
            from_c = safe_get('CompanyFrom').upper()
            from_s = safe_get('Street1From').upper()
            from_s2= safe_get('Street2From').upper()
            from_ci= safe_get('CityFrom').upper()
            from_st= safe_get('StateFrom').upper()
            from_z = safe_get('PostalCodeFrom').upper()
            
            to_n = safe_get('ToName').upper()
            to_c = safe_get('Company2').upper()
            to_s = safe_get('Street1To').upper()
            to_s2= safe_get('Street2To').upper()
            to_ci= safe_get('CityTo').upper()
            to_st= safe_get('StateTo').upper()
            
            try:
                raw_w = safe_get('Weight', '1')
                weight_val = float(raw_w)
            except: weight_val = 1.0; raw_w = '1'

            # --- TEMPLATE SELECTION ---
            if "stamps_v2" in t_choice:
                lbl = templates['heavy'] if weight_val >= 10 else templates['base']
            else:
                lbl = templates['default']
            
            zip_5 = to_z[:5] 
            zone = self.calculate_zone(from_st, to_st)
            days = self.calculate_transit_days(zone)
            exp_date = (now + timedelta(days=days)).strftime("%m/%d/%Y")
            mailer_id = self.get_mailer_id(version, data_folder)
            trk = self.generate_unique_tracking(version, mailer_id, batch_seq_code)
            acc, sec = self.generate_random_account_info()
            cr_route = self.generate_carrier_route(zip_5)
            
            if "easypost" in t_choice: acc = self.generate_c_number() 

            # --- WEIGHT TEXT LOGIC (ROBUST) ---
            if "stamps_v2" in t_choice:
                # SPECIAL STAMPS FORMAT: SPACES FOR R LOGO
                w_text = f"{weight_val:.1f} LB PRIORITY MAIL      RATE"
            elif "easypost" in t_choice:
                w_text = str(raw_w) # Just the number
            else:
                w_text = f"{weight_val:.1f} LB" # Pitney Format

            # --- ADDRESS FORMATTING ---
            sender_block = self.format_address(from_n, from_c, from_s, from_s2, from_ci, from_st, from_z)
            receiver_block = self.format_address(to_n, to_c, to_s, to_s2, to_ci, to_st, to_z)

            # Common Replacements
            lbl = lbl.replace("{SENDER_BLOCK}", sender_block).replace("{RECEIVER_BLOCK}", receiver_block)
            lbl = lbl.replace("{SHIP_DATE}", today).replace("{EXPECTED_DATE}", exp_date).replace("{ZONE_ID}", zone)
            lbl = lbl.replace("{FROM_ZIP}", from_z).replace("{ZIP_TO_5}", zip_5)
            lbl = lbl.replace("{FROM_ZIP_5}", from_z[:5]) 
            
            lbl = lbl.replace("{WEIGHT}", str(raw_w))        
            lbl = lbl.replace("{WEIGHT_TEXT}", w_text)  
            lbl = lbl.replace("{WEIGHT_RATE_TEXT}", w_text) 
            
            lbl = lbl.replace("{ACCOUNT_ID}", acc).replace("{SEC_REF}", sec).replace("{JULIAN_SEQ}", batch_seq_code if batch_seq_code else "000")

            dm_data = f"_1420{zip_5}_1{trk}"
            gs1_data = f">;>8420{zip_5}>8{trk}"
            
            lbl = lbl.replace("{BARCODE_DATA_DM}", dm_data).replace("{BARCODE_DATA_128}", gs1_data)
            lbl = lbl.replace("{TRACKING_SPACED}", " ".join([trk[i:i+4] for i in range(0, len(trk), 4)]))
            lbl = lbl.replace("{TRACKING_NO_SPACED}", trk)
            
            # REF Handling
            lbl = lbl.replace("{REF1}", sku).replace("{REF}", sku)
            lbl = lbl.replace("{REF2}", order_id).replace("{DESC}", desc_val)
            lbl = lbl.replace("{REFS_REORDERED}", f"{order_id} | {desc_val} | {sku}")
            
            lbl = lbl.replace("{CARRIER_ROUTE}", cr_route).replace("{SHIP_DATE_YMD}", now.strftime("%Y-%m-%d"))
            lbl = lbl.replace("{C_NUMBER}", acc).replace("{RANDOM_0901}", self.generate_0901_number())
            lbl = lbl.replace("{SHIP_DATE_YMD_NODASH}", now.strftime("%Y%m%d"))
            
            # --- PDF 417 BARCODE GENERATION ---
            if "pitney" in t_choice or "easypost" in t_choice or "stamps" in t_choice:
                oz4 = f"{int(weight_val * 16):04d}" 
                ymd = now.strftime("%Y%m%d")
                check_val = str(random.randint(10000000, 99999999)) 
                
                # STAMPS SPECIFIC VARS
                stamps_ref1 = "063S" + str(random.randint(1000000000, 9999999999))
                stamps_ref2 = str(random.randint(1000000, 9999999))
                
                lbl = lbl.replace("{STAMPS_REF_1}", stamps_ref1)
                lbl = lbl.replace("{STAMPS_REF_2}", stamps_ref2)

                # USE UNIVERSAL FORMAT FOR ALL THREE NOW
                pdf_acc_val = stamps_ref1 if "stamps" in t_choice else acc
                
                universal_pdf = f"USPS|PC|PM|Z{zone}|W{oz4}|D{ymd}|F{from_z[:5]}|T{zip_5}|S{trk}|RCOMM|A{pdf_acc_val}|C{check_val}|N0003|LCO25"
                lbl = lbl.replace("{PDF_417_DATA}", universal_pdf)

            else:
                # Fallback for old templates
                pdf_data = f"[)>^RS01420{zip_5}{acc}PM{raw_w}Z{zone}{now.strftime('%Y%m%d')}"
                lbl = lbl.replace("{PDF_417_DATA}", pdf_data)

            # --- SIZE LOGIC ---
            label_size = "4x6"
            if "stamps" in t_choice or "pitney" in t_choice or "easypost" in t_choice: 
                label_size = "4.8x7.1" 

            # --- DEBUG FILE WRITE ---
            try:
                debug_file = os.path.join(data_folder, "zpl_debug_log.txt")
                with open(debug_file, "a", encoding="utf-8") as f:
                    f.write(f"\n{'='*50}\nTIMESTAMP: {datetime.now()}\nORDER: {order_id} | TEMPLATE: {template_choice}\n{'-'*50}\n{lbl}\n{'='*50}\n")
            except: pass

            # --- API CALL ---
            attempts = 0
            max_retries = 10
            while attempts < max_retries:
                try:
                    res = requests.post(f"http://api.labelary.com/v1/printers/8dpmm/labels/{label_size}/", 
                                        data=lbl.encode('utf-8'), headers={'Accept': 'application/pdf'}, timeout=30)
                    
                    if res.status_code == 200:
                        # print(f" > [200 OK] Label Generated for {order_id}")
                        return res.content, {
                            "tracking": trk, "ref_id": sku, "from_name": from_n, "to_name": to_n,
                            "address_to": f"{to_s} {to_ci} {to_st} {to_z}", "ref02": order_id
                        }
                    elif res.status_code == 429:
                        time.sleep((attempts + 1) * 1.5)
                        attempts += 1
                        continue
                    else:
                        attempts += 1
                        time.sleep(1)
                except Exception as req_err:
                    attempts += 1
                    time.sleep(1)
            
            print(f" > [FAIL] Gave up on {order_id}")
            return None, None
        except Exception as e: 
            print(f" > [CRASH] {e}")
            return None, None

    def process_batch(self, df, label_type, version, batch_id, db_path, user_id, template_choice="pitney_v2", data_folder=None):
        merger = PdfWriter()
        df.columns = [str(c).strip().replace('\ufeff', '') for c in df.columns]

        if not data_folder:
            try: data_folder = current_app.config['DATA_FOLDER']
            except: raise Exception("No Data Folder provided to Engine")

        templates = {}
        try:
            if "stamps_v2" in template_choice:
                base_path = os.path.join(data_folder, 'zpl_templates', 'stamps_v2.zpl')
                heavy_path = os.path.join(data_folder, 'zpl_templates', 'stamps_v2_2digit.zpl')
                if os.path.exists(base_path):
                    with open(base_path, 'r', encoding='utf-8') as f: templates['base'] = f.read().strip()
                else: templates['base'] = ""
                if os.path.exists(heavy_path):
                    with open(heavy_path, 'r', encoding='utf-8') as f: templates['heavy'] = f.read().strip()
                else: templates['heavy'] = templates['base']
            else:
                if not template_choice.endswith('.zpl'): template_choice += ".zpl"
                zpl_path = os.path.join(data_folder, 'zpl_templates', template_choice)
                if not os.path.exists(zpl_path): 
                    raise Exception(f"Template missing: {zpl_path}")
                with open(zpl_path, 'r', encoding='utf-8') as f: templates['default'] = f.read().strip()
        except Exception as e:
            raise e

        now = datetime.now()
        today = now.strftime("%m/%d/%Y")
        ver_str = str(version).strip()
        batch_seq_code = now.strftime('%j')

        success_count = 0
        db_records = []
        
        # --- RE-ADDED CHUNKED UPDATES TO FIX 'REAL-TIME' ISSUE SAFELY ---
        # We perform 1 quick DB write every 5 labels.
        # This keeps the UI feeling 'live' without locking the DB for every single label.
        
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = {executor.submit(self.process_single_label, row, version, templates, template_choice, batch_seq_code, now, today, data_folder): idx for idx, row in df.iterrows()}
            
            for future in as_completed(futures):
                pdf_content, meta = future.result()
                if pdf_content and meta:
                    merger.append(io.BytesIO(pdf_content))
                    success_count += 1
                    
                    db_records.append((
                        batch_id, user_id, meta['ref_id'], meta['tracking'], "COMPLETED", 
                        meta['from_name'], meta['to_name'], meta['address_to'], version, 
                        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), meta['ref02']
                    ))

                    # --- SAFE LIVE UPDATE (CHUNKED EVERY 5) ---
                    if success_count % 5 == 0:
                        try:
                            # Short timeout to fail fast rather than block
                            conn = sqlite3.connect(db_path, timeout=1) 
                            conn.execute("UPDATE batches SET success_count = ? WHERE batch_id = ?", (success_count, batch_id))
                            conn.commit()
                            conn.close()
                        except: pass 

        if db_records:
            try:
                conn = sqlite3.connect(db_path, timeout=30)
                c = conn.cursor()
                try: c.execute("ALTER TABLE history ADD COLUMN ref02 TEXT"); conn.commit()
                except: pass
                c.executemany("INSERT INTO history (batch_id, user_id, ref_id, tracking, status, from_name, to_name, address_to, version, created_at, ref02) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", db_records)
                conn.commit(); conn.close()
            except: pass

        final_pdf = io.BytesIO()
        if success_count > 0: merger.write(final_pdf)
        return final_pdf.getvalue(), success_count