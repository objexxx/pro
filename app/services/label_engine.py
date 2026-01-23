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
from flask import current_app
import pandas as pd

class LabelEngine:
    def __init__(self):
        self.mode = "production"
        self.debug_mode = False 

    def sanitize_zpl(self, text):
        if not text: return ""
        return str(text).replace("^", "").replace("~", "")

    def get_mailer_id(self, version):
        json_path = os.path.join(current_app.config['DATA_FOLDER'], 'mailer_ids.json')
        default_ids = ['90000000'] 
        if not os.path.exists(json_path): return random.choice(default_ids)
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
            ver_str = str(version).strip()
            if "94888" in ver_str: mids = data.get('ids_94888', [])
            else: mids = data.get('ids_95055', [])
            if not mids: mids = data.get('mailer_ids', default_ids)
            return str(random.choice(mids)).strip()
        except: return random.choice(default_ids)

    def calculate_usps_check_digit(self, body):
        s = str(body)
        total = sum(int(c) for i, c in enumerate(s) if i % 2 == 0) * 3 + sum(int(c) for i, c in enumerate(s) if i % 2 != 0)
        return "0" if total % 10 == 0 else str(10 - (total % 10))

    def generate_unique_tracking(self, version, mailer_id, seq_code=None):
        stc = "9505"
        ver_str = str(version).strip()
        if "94888" in ver_str:
            stc = "94888"
            serial = f"{random.randint(1000000, 9999999)}"
        elif seq_code:
            suffix = f"{random.randint(0, 99999):05d}"
            serial = f"{seq_code}{suffix}"
        else:
            serial = f"{random.randint(0, 99999999):08d}"
        
        body = f"{stc}{mailer_id}{serial}"
        return body + self.calculate_usps_check_digit(body)
        
    def get_region(self, state):
        state = str(state).strip().upper()
        regions = {
            'West': ['CA', 'OR', 'WA', 'NV', 'AZ', 'ID', 'UT', 'HI', 'AK'],
            'Mountain': ['MT', 'WY', 'CO', 'NM', 'ND', 'SD', 'NE', 'KS', 'OK'],
            'Midwest': ['MN', 'IA', 'MO', 'WI', 'IL', 'MI', 'IN', 'OH', 'KY'],
            'South': ['TX', 'AR', 'LA', 'MS', 'AL', 'TN', 'GA', 'FL', 'SC', 'NC', 'VA', 'WV'],
            'Northeast': ['PA', 'NY', 'VT', 'NH', 'ME', 'MA', 'RI', 'CT', 'NJ', 'DE', 'MD', 'DC']
        }
        for r, s in regions.items():
            if state in s: return r
        return 'Midwest'

    def calculate_zone(self, state_from, state_to):
        r1 = self.get_region(state_from)
        r2 = self.get_region(state_to)
        if r1 == r2: return "2" if str(state_from).upper() == str(state_to).upper() else "3"
        reg_map = {'West': 0, 'Mountain': 1, 'Midwest': 2, 'South': 3, 'Northeast': 4}
        dist = abs(reg_map[r1] - reg_map[r2])
        return str(dist + 4) if dist < 4 else "8"

    def calculate_transit_days(self, zone):
        z = int(zone)
        return random.randint(1, 2) if z <= 2 else random.randint(2, 3) if z == 3 else random.randint(3, 4) if z <= 5 else random.randint(4, 5)

    def generate_carrier_route(self, zip_code):
        try:
            rng = random.Random(int(str(zip_code)[:5]))
            return f"{rng.choices(['C', 'R'], weights=[90, 10])[0]}{rng.randint(1, 99):03d}"
        except: return "C001"

    def format_address(self, name, company, street, city, state, zip_val):
        def clean(val):
            if not val or str(val).lower() == 'nan': return ""
            return self.sanitize_zpl(str(val).strip())
        parts = []
        if clean(name): parts.append(clean(name))
        if clean(company): parts.append(clean(company))
        if clean(street): parts.append(clean(street))
        csz = f"{clean(city)} {clean(state)} {clean(zip_val)}".strip()
        if csz: parts.append(csz)
        return "\\&".join(parts)

    def generate_0901_number(self): return f"090100000{random.randint(1000, 9999)}"
    def generate_random_account_info(self): return f"028W{random.randint(1000000000, 9999999999)}", str(random.randint(3000000000, 3999999999))
    def generate_c_number(self): return f"C{random.randint(1000000, 9999999)}"

    def generate_stamps_refs(self):
        ref1 = f"063S00000{random.randint(10000, 99999)}"
        ref2 = f"{random.randint(1000000, 9999999)}"
        return ref1, ref2

    def process_batch(self, df, label_type, version, batch_id, db_path, user_id, template_choice="pitney_v2"):
        success_count = 0
        merger = PdfWriter()
        df.columns = [str(c).strip().replace('\ufeff', '') for c in df.columns]

        templates = {}
        if "stamps_v2" in template_choice:
            base_name = "stamps_v2.zpl"
            heavy_name = "stamps_v2_2digit.zpl"
            base_path = os.path.join(current_app.config['DATA_FOLDER'], 'zpl_templates', base_name)
            heavy_path = os.path.join(current_app.config['DATA_FOLDER'], 'zpl_templates', heavy_name)
            if os.path.exists(base_path):
                with open(base_path, 'r', encoding='utf-8') as f: templates['base'] = f.read().strip()
            else: templates['base'] = "" 
            if os.path.exists(heavy_path):
                with open(heavy_path, 'r', encoding='utf-8') as f: templates['heavy'] = f.read().strip()
            else: templates['heavy'] = templates['base']
        else:
            if not template_choice.endswith('.zpl'): template_choice += ".zpl"
            zpl_path = os.path.join(current_app.config['DATA_FOLDER'], 'zpl_templates', template_choice)
            if not os.path.exists(zpl_path): raise Exception(f"Template missing: {zpl_path}")
            with open(zpl_path, 'r', encoding='utf-8') as f: templates['default'] = f.read().strip()

        now = datetime.now()
        today = now.strftime("%m/%d/%Y")
        ver_str = str(version).strip()
        batch_seq_code = None if "94888" in ver_str else now.strftime('%j')

        debug_filename = f"zpl_debug_{batch_id}.txt"
        debug_path = os.path.join(current_app.config['DATA_FOLDER'], 'uploads', debug_filename)
        
        if self.debug_mode:
            with open(debug_path, 'w', encoding='utf-8') as dbg:
                dbg.write(f"DEBUG LOG FOR BATCH {batch_id}\nDate: {now}\n" + "="*50 + "\n\n")

        try:
            conn = sqlite3.connect(db_path, timeout=10); c = conn.cursor()
            try: c.execute("ALTER TABLE history ADD COLUMN ref02 TEXT"); conn.commit()
            except: pass
            conn.close()
        except: pass

        for idx, row in df.iterrows():
            attempts = 0
            label_generated = False
            
            def safe_get(key, default=""):
                val = row.get(key)
                if pd.isna(val) or str(val).lower() == 'nan': return default
                return self.sanitize_zpl(str(val).strip())

            order_id = safe_get('Ref01'); sku = safe_get('Ref02'); desc_val = safe_get('Description')
            from_n = safe_get('FromName'); from_c = safe_get('CompanyFrom'); from_s = safe_get('Street1From')
            from_ci= safe_get('CityFrom'); from_st= safe_get('StateFrom'); from_z = safe_get('PostalCodeFrom')
            to_n = safe_get('ToName'); to_c = safe_get('Company2'); to_s = safe_get('Street1To')
            to_ci= safe_get('CityTo'); to_st= safe_get('StateTo'); to_z = safe_get('ZipTo')
            
            try:
                raw_w = safe_get('Weight', '1')
                weight_val = float(raw_w)
            except: weight_val = 1.0; raw_w = '1'

            while attempts < 3 and not label_generated:
                try:
                    time.sleep(0.35)
                    
                    if "stamps_v2" in template_choice:
                        lbl = templates['heavy'] if weight_val >= 10 else templates['base']
                    else:
                        lbl = templates['default']
                    
                    if "Weight lbs 0 ozs" in lbl: lbl = lbl.replace("Weight lbs 0 ozs", "{WEIGHT}")
                    
                    zip_5 = to_z[:5] 
                    zone = self.calculate_zone(from_st, to_st)
                    days = self.calculate_transit_days(zone)
                    exp_date = (now + timedelta(days=days)).strftime("%m/%d/%Y")
                    mailer_id = self.get_mailer_id(version)
                    trk = self.generate_unique_tracking(version, mailer_id, batch_seq_code)
                    acc, sec = self.generate_random_account_info()
                    cr_route = self.generate_carrier_route(zip_5)
                    
                    if "easypost" in template_choice.lower(): acc = self.generate_c_number() 
                    
                    if "stamps_v2" in template_choice: w_disp = raw_w
                    elif "easypost" in template_choice.lower(): w_disp = raw_w
                    else: w_disp = f"{raw_w} Lbs 0 ozs"

                    sender_block = self.format_address(from_n, from_c, from_s, from_ci, from_st, from_z)
                    receiver_block = self.format_address(to_n, to_c, to_s, to_ci, to_st, to_z)

                    lbl = lbl.replace("{SENDER_BLOCK}", sender_block).replace("{RECEIVER_BLOCK}", receiver_block)
                    lbl = lbl.replace("{SHIP_DATE}", today).replace("{EXPECTED_DATE}", exp_date).replace("{ZONE_ID}", zone)
                    lbl = lbl.replace("{FROM_ZIP}", from_z).replace("{ZIP_TO_5}", zip_5).replace("{WEIGHT}", w_disp) 
                    lbl = lbl.replace("{ACCOUNT_ID}", acc).replace("{SEC_REF}", sec).replace("{JULIAN_SEQ}", batch_seq_code if batch_seq_code else "000")

                    # --- UPDATED BARCODE LOGIC FOR DATA MATRIX AND GS1-128 ---
                    dm_data = f"_1420{zip_5}_1{trk}"
                    gs1_data = f">;>8420{zip_5}>8{trk}"
                    
                    lbl = lbl.replace("{BARCODE_DATA_DM}", dm_data).replace("{BARCODE_DATA_128}", gs1_data)
                    lbl = lbl.replace("{TRACKING_SPACED}", " ".join([trk[i:i+4] for i in range(0, len(trk), 4)]))
                    lbl = lbl.replace("{TRACKING_NO_SPACED}", trk)
                    
                    lbl = lbl.replace("{REF1}", sku).replace("{REF2}", order_id).replace("{DESC}", desc_val)
                    lbl = lbl.replace("{REFS_REORDERED}", f"{order_id} | {desc_val} | {sku}")
                    lbl = lbl.replace("{CARRIER_ROUTE}", cr_route).replace("{SHIP_DATE_YMD}", now.strftime("%Y-%m-%d"))
                    lbl = lbl.replace("{C_NUMBER}", acc).replace("{RANDOM_0901}", self.generate_0901_number())
                    lbl = lbl.replace("{SHIP_DATE_YMD_NODASH}", now.strftime("%Y%m%d"))
                    
                    if "stamps_v2" in template_choice:
                        stamps_ref1, stamps_ref2 = self.generate_stamps_refs()
                        lbl = lbl.replace("{STAMPS_REF_1}", stamps_ref1).replace("{STAMPS_REF_2}", stamps_ref2)
                        
                        try: w_str = f"{int(weight_val * 100):04d}"
                        except: w_str = "0100"
                        
                        stamps_pdf_data = f"USPS|PC|PM|Z{zone}|W{w_str}|D{now.strftime('%Y%m%d')}|F{from_z}|T{zip_5}|S{trk}|RCOMM|A{stamps_ref1}|C{stamps_ref2}|N0003|LCO25"
                        lbl = lbl.replace("{PDF_417_DATA}", stamps_pdf_data)
                    else:
                        pdf_data = f"[)>^RS01420{zip_5}{acc}PM{raw_w}Z{zone}{now.strftime('%Y%m%d')}"
                        lbl = lbl.replace("{PDF_417_DATA}", pdf_data)

                    if self.debug_mode:
                        with open(debug_path, 'a', encoding='utf-8') as dbg:
                            dbg.write(f"\n--- LABEL {idx + 1} ---\nTRACKING: {trk}\nPDF_DATA: {stamps_pdf_data if 'stamps' in template_choice else pdf_data}\n")

                    label_size = "4.5x6.7" if "stamps" in template_choice.lower() else "4x6"

                    res = requests.post(f"http://api.labelary.com/v1/printers/8dpmm/labels/{label_size}/", 
                                        data=lbl.encode('utf-8'), headers={'Accept': 'application/pdf'}, timeout=30)

                    if res.status_code == 200:
                        merger.append(io.BytesIO(res.content))
                        success_count += 1
                        label_generated = True 
                        try:
                            conn = sqlite3.connect(db_path, timeout=10); c = conn.cursor()
                            c.execute("UPDATE batches SET success_count = success_count + 1 WHERE batch_id = ?", (batch_id,))
                            c.execute("INSERT INTO history (batch_id, user_id, ref_id, tracking, status, from_name, to_name, address_to, version, created_at, ref02) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                                      (batch_id, user_id, sku, trk, "COMPLETED", from_n, to_n, f"{to_s} {to_ci} {to_st} {to_z}", version, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), order_id))
                            conn.commit(); conn.close()
                        except: pass
                    else: attempts += 1
                except: attempts += 1

            if not label_generated:
                try:
                    conn = sqlite3.connect(db_path, timeout=10); c = conn.cursor()
                    c.execute("INSERT INTO history (batch_id, user_id, ref_id, tracking, status, from_name, to_name, address_to, version, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                              (batch_id, user_id, "FAILED", "FAILED", "FAILED", from_n, to_n, "Error", version, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
                    conn.commit(); conn.close()
                except: pass

        try:
            conn = sqlite3.connect(db_path, timeout=10); c = conn.cursor()
            final_status = "COMPLETED" if success_count > 0 else "FAILED"
            c.execute("UPDATE batches SET status = ? WHERE batch_id = ?", (final_status, batch_id))
            conn.commit(); conn.close()
        except Exception as e: print(f"Status Update Error: {e}")

        final_pdf = io.BytesIO()
        if success_count > 0: merger.write(final_pdf)
        return final_pdf.getvalue(), success_count