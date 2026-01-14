import random

def get_region(state):
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

def calculate_zone(state_from, state_to):
    r1 = get_region(state_from)
    r2 = get_region(state_to)
    if r1 == r2: return "2" if str(state_from).upper() == str(state_to).upper() else "3"
    reg_map = {'West': 0, 'Mountain': 1, 'Midwest': 2, 'South': 3, 'Northeast': 4}
    dist = abs(reg_map[r1] - reg_map[r2])
    return str(dist + 4) if dist < 4 else "8"

def calculate_transit_days(zone):
    z = int(zone)
    return random.randint(1, 2) if z <= 2 else random.randint(2, 3) if z == 3 else random.randint(3, 4) if z <= 5 else random.randint(4, 5)

def calculate_usps_check_digit(body):
    s = str(body)
    total = sum(int(c) for i, c in enumerate(s) if i % 2 == 0) * 3 + sum(int(c) for i, c in enumerate(s) if i % 2 != 0)
    return "0" if total % 10 == 0 else str(10 - (total % 10))

def generate_unique_tracking(version_prefix="9505", mailer_id=None):
    stc = str(version_prefix).strip()
    if not mailer_id:
        mailer_id = random.choice(['5', '6']) + f"{random.randint(10000000, 99999999)}"
    if stc == "94888": serial = f"{random.randint(1000000, 9999999)}"
    elif stc == "95055_random": stc = "9505"; serial = f"{random.randint(0, 99999999):08d}"
    else: stc = "9505"; serial = "005" + f"{random.randint(10000, 99999)}"
    body = stc + mailer_id + serial
    return body + calculate_usps_check_digit(body)

def format_address(name, company, street, city, state, zip_val):
    parts = [str(name).strip()]
    if str(company).strip() and str(company).lower() != "nan": parts.append(str(company).strip())
    parts.append(str(street).strip())
    parts.append(f"{str(city).strip()} {str(state).strip()} {str(zip_val).strip()}")
    return "\\&".join(parts)
    
def generate_random_account_info():
    return f"028W{random.randint(1000000000, 9999999999)}", str(random.randint(3000000000, 3999999999))

def generate_c_number(): return f"C{random.randint(1000000, 9999999)}"
def generate_0901_number(): return f"090100000{random.randint(1000, 9999)}"
def generate_carrier_route(zip_code): return "C001"