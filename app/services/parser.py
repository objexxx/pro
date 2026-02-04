import csv
import io
import zipfile
import json
import re
import pandas as pd
from datetime import datetime

# --- SMART STATE MAPPING ---
US_STATES_MAP = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR", "CALIFORNIA": "CA",
    "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE", "FLORIDA": "FL", "GEORGIA": "GA",
    "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA",
    "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
    "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS", "MISSOURI": "MO",
    "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV", "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH",
    "OKLAHOMA": "OK", "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT",
    "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY",
    "DISTRICT OF COLUMBIA": "DC", "PUERTO RICO": "PR", "GUAM": "GU", "AMERICAN SAMOA": "AS", "VIRGIN ISLANDS": "VI"
}

class OrderParser:
    @staticmethod
    def parse_to_zip(file_content, inventory_json, sender_profile=None):
        try:
            # Decode file content
            text_content = file_content.decode('utf-8-sig')
            
            # --- DETECT DELIMITER (Tab vs Comma) ---
            delimiter = ','
            first_line = text_content.splitlines()[0] if text_content else ''
            if '\t' in first_line:
                delimiter = '\t'
            
            input_csv = csv.DictReader(io.StringIO(text_content), delimiter=delimiter)
            
            # Parse Inventory Map
            inventory_map = {}
            if inventory_json:
                try:
                    inventory_map = json.loads(inventory_json)
                except:
                    pass

            # Dictionary to group orders by SKU
            orders_by_sku = {}

            # Output Headers
            fieldnames = [
                'No', 'FromName', 'PhoneFrom', 'Street1From', 'CompanyFrom', 'Street2From', 
                'CityFrom', 'StateFrom', 'PostalCodeFrom', 'ToName', 'PhoneTo', 'Street1To', 
                'Company2', 'Street2To', 'CityTo', 'StateTo', 'ZipTo', 'Weight', 'Length', 
                'Width', 'Height', 'Description', 'Ref01', 'Ref02', 'Contains Hazard', 'Shipment Date'
            ]
            
            row_count = 0
            
            for row in input_csv:
                # --- SMART STATE AUTO-CORRECTION ---
                def smart_state(val):
                    if not val: return ""
                    clean_val = str(val).strip().upper()
                    return US_STATES_MAP.get(clean_val, clean_val)

                # Recipient State
                raw_state_to = row.get('Shipping State') or row.get('State') or row.get('ship-state') or row.get('Recipient State') or ''
                final_state_to = smart_state(raw_state_to)

                # Sender State
                raw_state_from = row.get('Return State') or ''
                final_state_from = smart_state(raw_state_from)

                # Sender Info (Profile takes priority)
                s_name = sender_profile['name'] if sender_profile else "Shipping Dept"
                s_comp = sender_profile['company'] if sender_profile else ""
                s_str1 = sender_profile['street1'] if sender_profile else "123 Sender Ln"
                s_str2 = sender_profile.get('street2', '') if sender_profile else ""
                s_city = sender_profile['city'] if sender_profile else "Las Vegas"
                s_state = sender_profile['state'] if sender_profile else "NV"
                s_zip = sender_profile['zip'] if sender_profile else "89101"
                s_phone = sender_profile['phone'] if sender_profile else "5551234567"

                # Parse Items/SKUs
                raw_sku = row.get('SKU') or row.get('Item SKU') or row.get('sku') or row.get('Seller SKU') or "Unknown_SKU"
                sku = str(raw_sku).strip() # Clean spaces
                
                # Sanitize SKU for filename usage later
                safe_sku = re.sub(r'[\\/*?:"<>|]', "_", sku).strip()
                if not safe_sku: safe_sku = "Unknown_SKU"

                # --- STRICT INVENTORY CHECK (REQUIRED) ---
                if sku not in inventory_map:
                    return None, f"SKU NOT MAPPED: {sku} (Please add this SKU to your Inventory in Automation settings)"

                # Load Inventory Data
                item_data = inventory_map[sku]
                
                # Default Dimensions & Description (Fallback if missing in JSON, though unlikely if keyed)
                weight = str(item_data.get('weight', "1"))
                length, width, height = "10", "6", "4"
                desc = ""
                
                # Check ALL possible key names for description from inventory
                inventory_desc = (
                    item_data.get('name') or 
                    item_data.get('description') or 
                    item_data.get('desc') or 
                    item_data.get('package_description')
                )
                
                if inventory_desc:
                    desc = inventory_desc
                
                # Recipient Mapping
                to_name = row.get('Recipient Name') or row.get('Name') or row.get('recipient-name') or row.get('Ship To Name') or ""
                to_phone = row.get('Ship To Phone') or row.get('Phone') or row.get('buyer-phone-number') or ""
                to_str1 = row.get('Ship To Address 1') or row.get('Address 1') or row.get('ship-address-1') or row.get('Street 1') or ""
                to_str2 = row.get('Ship To Address 2') or row.get('Address 2') or row.get('ship-address-2') or row.get('Street 2') or ""
                to_city = row.get('Ship To City') or row.get('City') or row.get('ship-city') or ""
                to_zip = row.get('Ship To Zip') or row.get('Postal Code') or row.get('ship-postal-code') or row.get('Zip') or ""
                
                order_id = row.get('Order ID') or row.get('Order Number') or row.get('order-id') or f"ORD-{row_count}"

                # --- QUANTITY EXPLOSION LOGIC ---
                # Detect quantity from CSV
                raw_qty = row.get('quantity-purchased') or row.get('quantity') or row.get('qty') or row.get('Quantity') or "1"
                try:
                    qty = int(raw_qty)
                except:
                    qty = 1
                
                # If quantity is 0 or negative, skip (or default to 1, depending on preference. Defaulting to 1 for safety)
                if qty < 1: qty = 1

                # Generate ONE ROW per QUANTITY Unit
                for _ in range(qty):
                    formatted_row = {
                        'No': str(row_count + 1),
                        'FromName': s_name,
                        'PhoneFrom': s_phone,
                        'Street1From': s_str1,
                        'CompanyFrom': s_comp,
                        'Street2From': s_str2,
                        'CityFrom': s_city,
                        'StateFrom': s_state, 
                        'PostalCodeFrom': s_zip,
                        'ToName': to_name,
                        'PhoneTo': to_phone,
                        'Street1To': to_str1,
                        'Company2': "",
                        'Street2To': to_str2,
                        'CityTo': to_city,
                        'StateTo': final_state_to,
                        'ZipTo': to_zip,
                        'Weight': weight,
                        'Length': length,
                        'Width': width,
                        'Height': height,
                        'Description': desc,
                        'Ref01': sku,
                        'Ref02': order_id,
                        'Contains Hazard': 'False',
                        'Shipment Date': datetime.now().strftime("%m/%d/%Y")
                    }

                    # Group by SKU
                    if safe_sku not in orders_by_sku:
                        orders_by_sku[safe_sku] = []
                    orders_by_sku[safe_sku].append(formatted_row)
                    
                    row_count += 1

            # Create ZIP containing multiple CSVs
            memory_file = io.BytesIO()
            with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
                for sku_name, rows in orders_by_sku.items():
                    sku_output = io.StringIO()
                    writer = csv.DictWriter(sku_output, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
                    
                    # Add CSV to ZIP with SKU as filename
                    zf.writestr(f"{sku_name}.csv", sku_output.getvalue())
            
            memory_file.seek(0)
            return memory_file.getvalue(), None

        except Exception as e:
            return None, str(e)

# ==========================================
#   NEW FUNCTIONS FOR BULK / WALMART
# ==========================================

def parse_walmart_xlsx(file_stream, sender_profile):
    try:
        # Load the XLSX file
        xls = pd.ExcelFile(file_stream)
        target_sheet = None
        for sheet_name in xls.sheet_names:
            if "po details" in sheet_name.lower():
                target_sheet = sheet_name
                break
        
        if not target_sheet:
            target_sheet = xls.sheet_names[0]

        df = pd.read_excel(xls, sheet_name=target_sheet)
        
        # Validation
        if len(df.columns) <= 25:
             print("Walmart XLSX: Not enough columns found.")
             return [], 0, 0.0

        # Sort by SKU (Column Z / Index 25)
        sku_col_name = df.columns[25] 
        df = df.sort_values(by=sku_col_name)
        
        data = []
        
        for index, row in df.iterrows():
            try:
                # --- 1. GHOST ROW KILLER ---
                raw_name = str(row.iloc[5]).strip()
                raw_street = str(row.iloc[8]).strip()
                
                # Skip empty rows
                if not raw_name or raw_name.lower() == 'nan' or not raw_street or raw_street.lower() == 'nan':
                    continue 

                # --- 2. QUANTITY LOGIC (Column Y / Index 24) ---
                try:
                    raw_qty = row.iloc[24]
                    qty = int(raw_qty) if pd.notna(raw_qty) else 1
                except:
                    qty = 1
                
                if qty < 1: qty = 1

                # --- 3. ZIP CODE FIX ---
                raw_zip = str(row.iloc[13]).strip()
                if raw_zip.endswith('.0'): raw_zip = raw_zip[:-2]
                final_zip = raw_zip.zfill(5)

                # --- 4. ORDER ID FIX (Column B / Index 1) ---
                # We need this ID to map tracking numbers back to the Excel file later
                order_number = str(row.iloc[1]).strip()
                if order_number.endswith('.0'): order_number = order_number[:-2]

                # --- 5. MAP DATA ---
                entry = {
                    'to_name': raw_name,
                    'to_phone': str(row.iloc[7]).strip() if pd.notna(row.iloc[7]) else "",
                    'to_street1': raw_street,
                    'to_street2': str(row.iloc[9]).strip() if pd.notna(row.iloc[9]) else "",
                    'to_city': str(row.iloc[11]).strip(),
                    'to_state': str(row.iloc[12]).strip(),
                    'to_zip': final_zip,
                    'to_country': 'US',
                    'weight': float(qty) * 1.0,  # 1 lb per item
                    'Ref01': str(row.iloc[25]).strip(), # SKU
                    'Ref02': order_number,              # WALMART ORDER NUMBER (Saved to DB)
                    'from_name': sender_profile.name,
                    'from_company': sender_profile.company,
                    'from_phone': sender_profile.phone,
                    'from_street1': sender_profile.street1,
                    'from_street2': sender_profile.street2,
                    'from_city': sender_profile.city,
                    'from_state': sender_profile.state,
                    'from_zip': sender_profile.zip
                }
                
                data.append(entry)

            except Exception as e:
                print(f"Skipping row {index}: {e}")
                continue
        
        return data, len(data), 0.0
        
    except Exception as e:
        print(f"Walmart Parsing Error: {e}")
        return [], 0, 0.0