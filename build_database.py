import pandas as pd
import json
import os
import shutil
import urllib.request
import ssl
import sys

# --- CONFIGURATION ---
INSTALLED_METERS_URL = "https://minisites.howazit.com/5430101017/mobility_addresses.csv"

# Directories
SOURCE_DIR = 'iec_sources'
OUTPUT_DIR = 'data'

# Local File Paths
INSTALLED_FILE = os.path.join(SOURCE_DIR, 'installed_meters.csv')
PLANNED_FILE = os.path.join(SOURCE_DIR, 'annual_plan_26.csv')

# --- SETUP ---
if not os.path.exists(SOURCE_DIR):
    os.makedirs(SOURCE_DIR)

if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR)

database = {}

# --- HELPER FUNCTIONS ---
def clean_val(val):
    """
    Cleans numeric values that might be read as floats (e.g., "8.0" -> "8")
    """
    s = str(val).strip()
    if s.endswith('.0'):
        return s[:-2]
    if s == 'nan':
        return ''
    return s

def download_file(url, target_path):
    print(f"Downloading update from: {url}")
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        with urllib.request.urlopen(url, context=ctx) as response:
            total_size = response.getheader('Content-Length')
            if total_size:
                total_size = int(total_size)
            
            with open(target_path, 'wb') as out_file:
                downloaded = 0
                block_size = 8192
                
                while True:
                    buffer = response.read(block_size)
                    if not buffer:
                        break
                    downloaded += len(buffer)
                    out_file.write(buffer)
                    
                    if total_size:
                        percent = downloaded * 100 / total_size
                        bar = '=' * int(40 * downloaded // total_size)
                        sys.stdout.write(f'\r   > Progress: [{bar:<40}] {percent:.1f}%')
                        sys.stdout.flush()
                        
        print(f"\n   > Saved to: {target_path}\n")
        return True
    except Exception as e:
        print(f"\n   > Download FAILED: {e}")
        return False

def load_csv_adaptive(filepath):
    """
    Tries multiple encodings AND delimiters (comma vs semicolon).
    Scans for the header row.
    """
    encodings = ['cp1255', 'utf-8', 'iso-8859-8', 'latin-1']
    
    for enc in encodings:
        try:
            # Step 1: Read lines to find header and detect delimiter
            with open(filepath, 'r', encoding=enc) as f:
                lines = [f.readline() for _ in range(15)]
            
            header_idx = -1
            delimiter = ',' # Default
            
            for i, line in enumerate(lines):
                # Heuristic: Check for Hebrew keywords
                if 'עיר' in line or 'רחוב' in line or 'City' in line:
                    header_idx = i
                    # Detect delimiter based on count in the header line
                    if line.count(';') > line.count(','):
                        delimiter = ';'
                    break
            
            if header_idx == -1:
                continue # Try next encoding
            
            print(f"   > Reading with encoding '{enc}', delimiter '{delimiter}', header at row {header_idx}")
            
            # Step 2: Read CSV with detected settings
            return pd.read_csv(filepath, header=header_idx, sep=delimiter, dtype=str, encoding=enc, on_bad_lines='skip')

        except Exception:
            continue
            
    return None

def find_column(df, keywords):
    for col in df.columns:
        for key in keywords:
            if key in str(col):
                return col
    return None

# --- MAIN EXECUTION ---

# 1. DOWNLOAD
print("--- Step 1: Updating Data Source ---")
if not download_file(INSTALLED_METERS_URL, INSTALLED_FILE):
    if not os.path.exists(INSTALLED_FILE):
        print("CRITICAL ERROR: Download failed and no local file exists.")
        exit(1)
    print("   > Using existing local file instead.")

# 2. PROCESS INSTALLED METERS
print("--- Step 2: Processing Installed Meters ---")
df_inst = load_csv_adaptive(INSTALLED_FILE)

if df_inst is None:
    print(f"CRITICAL ERROR: Could not read {INSTALLED_FILE}")
    exit(1)

city_col = find_column(df_inst, ['עיר', 'City'])
street_col = find_column(df_inst, ['רחוב', 'Street'])
num_col = find_column(df_inst, ['בית', 'House', 'Number', 'מס'])

if not (city_col and street_col and num_col):
    print("ERROR: Columns not found in Installed file.")
    # Debug print to see what went wrong
    print(f"Columns found: {df_inst.columns.tolist()}")
    exit(1)

for _, row in df_inst.iterrows():
    city = clean_val(row[city_col])
    street = clean_val(row[street_col])
    num = clean_val(row[num_col])
    
    if not city or not street: continue
    if city not in database: database[city] = {}
    if street not in database[city]: database[city][street] = {'i': [], 'p': {}}
    
    database[city][street]['i'].append(num)

# 3. PROCESS PLANNED METERS
print("--- Step 3: Processing Planned 2026 Meters ---")
if os.path.exists(PLANNED_FILE):
    df_plan = load_csv_adaptive(PLANNED_FILE)
    
    if df_plan is not None:
        city_col = find_column(df_plan, ['עיר', 'City'])
        street_col = find_column(df_plan, ['רחוב', 'Street'])
        num_col = find_column(df_plan, ['בית', 'House', 'Number', 'מס'])
        quart_col = find_column(df_plan, ['רבעון', 'Quarter', 'Rivon'])

        if (city_col and street_col and num_col and quart_col):
            for _, row in df_plan.iterrows():
                city = clean_val(row[city_col])
                street = clean_val(row[street_col])
                num = clean_val(row[num_col])
                quarter = clean_val(row[quart_col])
                
                if not city or not street: continue
                if city not in database: database[city] = {}
                if street not in database[city]: database[city][street] = {'i': [], 'p': {}}
                
                database[city][street]['p'][num] = quarter
        else:
            print("   > Warning: Columns missing in planned file.")
    else:
        print("   > Warning: Could not read planned file.")
else:
    print(f"   > Note: Planned file '{PLANNED_FILE}' not found. Skipping.")

# 4. GENERATE SHARDS
print("--- Step 4: Generating Database Shards ---")
city_list = sorted(list(database.keys()))

with open(f'{OUTPUT_DIR}/cities.json', 'w', encoding='utf-8') as f:
    json.dump(city_list, f, ensure_ascii=False)

count = 0
for city, streets in database.items():
    safe_filename = city.replace('"', '').replace('/', '-').replace('\\', '-').strip()
    if not safe_filename: continue
    
    with open(f'{OUTPUT_DIR}/{safe_filename}.json', 'w', encoding='utf-8') as f:
        json.dump(streets, f, ensure_ascii=False, separators=(',', ':'))
    count += 1

print(f"\nSUCCESS! Database updated with {count} cities in '{OUTPUT_DIR}/'.")
