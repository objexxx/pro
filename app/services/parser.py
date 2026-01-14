import csv
import io
import datetime
import json
import zipfile
from collections import defaultdict

class OrderParser:
    @staticmethod
    def parse_to_zip(unshipped_content, inventory_json, sender_address):
        # 1. Strict Check for Address
        if not sender_address:
            return None, "NO SENDER ADDRESS SELECTED"

        try:
            inventory = json.loads(inventory_json)
        except:
            return None, "Invalid Inventory JSON format"

        # Decode File
        try:
            decoded = unshipped_content.decode("utf-8-sig")
        except:
            decoded = unshipped_content.decode("latin-1")

        stream = io.StringIO(decoded)
        
        # Detect Delimiter (Tab for Amazon txt, Comma for others)
        reader = csv.DictReader(stream, delimiter='\t')
        
        # If headers look wrong, retry with comma
        if not reader.fieldnames or len(reader.fieldnames) < 2:
            stream.seek(0)
            reader = csv.DictReader(stream, delimiter=',')

        # --- FIX 1: Normalize Input Headers ---
        if reader.fieldnames:
            reader.fieldnames = [h.strip() for h in reader.fieldnames]

        # --- VALIDATION STEP ---
        all_rows = list(reader)
        
        missing_skus = set()
        
        # 1. Scan for Missing SKUs
        for row in all_rows:
            sku = row.get('sku', '').replace('"', '').strip()
            if sku and sku not in inventory:
                missing_skus.add(sku)
        
        # 2. Fail if any are missing
        if missing_skus:
            missing_list = ", ".join(sorted(list(missing_skus)))
            return None, f"MISSING SKUS IN INVENTORY: {missing_list}"

        # --- PROCESSING STEP ---
        grouped_rows = defaultdict(list)
        
        # --- NEW STRICT HEADERS ---
        headers = [
            'No', 'FromName', 'PhoneFrom', 'Street1From', 'CompanyFrom', 
            'Street2From', 'CityFrom', 'StateFrom', 'PostalCodeFrom', 
            'ToName', 'PhoneTo', 'Street1To', 'Company2', 'Street2To', 
            'CityTo', 'StateTo', 'ZipTo', 'Weight', 'Length', 
            'Width', 'Height', 'Description', 'Ref01', 'Ref02', 
            'Contains Hazard', 'Shipment Date'
        ]

        row_counter = 1
        addr = sender_address

        for row in all_rows:
            sku = row.get('sku', '').replace('"', '').strip()
            
            if sku not in inventory: continue 
            
            item_data = inventory[sku]
            weight = int(item_data.get('weight', 1))
            desc = item_data.get('description', 'Merchandise')
            
            try:
                qty = int(row.get('quantity-purchased', 1))
            except:
                qty = 1

            for _ in range(qty):
                # Clean phone number
                phone = row.get('buyer-phone-number', '')[:15] 

                # --- MAPPING TO NEW HEADER NAMES ---
                new_row = {
                    'No': row_counter,
                    'FromName': addr.get('name', ''),
                    'PhoneFrom': addr.get('phone', ''),
                    'Street1From': addr.get('street1', ''),
                    'CompanyFrom': addr.get('company', ''),
                    'Street2From': addr.get('street2', ''),
                    'CityFrom': addr.get('city', ''),
                    'StateFrom': addr.get('state', ''),
                    'PostalCodeFrom': addr.get('zip', ''),
                    'ToName': row.get('recipient-name', '')[:30],
                    'PhoneTo': phone,
                    'Street1To': row.get('ship-address-1', ''),
                    'Company2': '', # Amazon usually puts company in address lines
                    'Street2To': row.get('ship-address-2', ''),
                    'CityTo': row.get('ship-city', ''),
                    'StateTo': row.get('ship-state', ''),
                    'ZipTo': row.get('ship-postal-code', ''), # Renamed from To PostalCode
                    'Weight': weight,
                    'Length': 10, 'Width': 6, 'Height': 1,
                    'Description': desc,
                    'Ref01': sku, # SKU maps to Ref01 now? Or swap if needed.
                    'Ref02': row.get('order-id', ''),
                    'Contains Hazard': 'FALSE', # Renamed from Contains Hazardous
                    'Shipment Date': datetime.datetime.now().strftime('%m/%d/%Y')
                }
                grouped_rows[sku].append(new_row)
                row_counter += 1

        if not grouped_rows:
            return None, "File appears empty or no SKUs found."

        # Create ZIP
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for sku, rows in grouped_rows.items():
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)
                
                safe_sku = "".join(c for c in sku if c.isalnum() or c in (' ', '-', '_')).strip()
                zip_file.writestr(f"{safe_sku}.csv", csv_buffer.getvalue())

        zip_buffer.seek(0)
        return zip_buffer.getvalue(), None