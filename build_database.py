import pandas as pd
import json
import os
import shutil

# --- CONFIGURATION ---
installed_file = 'iec_sources/installed_meters.csv'
planned_file = 'iec_sources/annual_plan_26.csv'
output_dir = 'data'

# 1. Reset Output Directory
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
os.makedirs(output_dir)

database = {}

def clean_val(val):
    # Convert to string, strip whitespace, and remove ".0" if pandas added it
    s = str(val).strip()
    if s.endswith('.0'):
        return s[:-2]
    return s

print("1. Processing Installed Meters...")
try:
    # dtype=str forces pandas to treat everything as text (prevents 8 -> 8.0)
    df = pd.read_csv(installed_file, dtype=str)
    
    city_col = [c for c in df.columns if 'עיר' in c][0]
    street_col = [c for c in df.columns if 'רחוב' in c][0]
    num_col = [c for c in df.columns if 'בית' in c][0]

    for _, row in df.iterrows():
        city = clean_val(row[city_col])
        street = clean_val(row[street_col])
        num = clean_val(row[num_col])
        
        if not city or not street: continue
        if city not in database: database[city] = {}
        if street not in database[city]: database[city][street] = {'i': [], 'p': {}}
        
        database[city][street]['i'].append(num)
except Exception as e:
    print(f"Error reading installed file: {e}")

print("2. Processing Planned 2026 Meters...")
try:
    # dtype=str forces pandas to treat everything as text
    df = pd.read_csv(planned_file, dtype=str)
    
    city_col = [c for c in df.columns if 'עיר' in c][0]
    street_col = [c for c in df.columns if 'רחוב' in c][0]
    num_col = [c for c in df.columns if 'בית' in c][0]
    # Flexible check for Quarter column
    quart_col = [c for c in df.columns if 'רבעון' in c or 'Quarter' in c][0]

    for _, row in df.iterrows():
        city = clean_val(row[city_col])
        street = clean_val(row[street_col])
        num = clean_val(row[num_col])
        quarter = clean_val(row[quart_col])
        
        if not city or not street: continue
        if city not in database: database[city] = {}
        if street not in database[city]: database[city][street] = {'i': [], 'p': {}}
        
        database[city][street]['p'][num] = quarter
except Exception as e:
    print(f"Error reading planned file: {e}")

print("3. Generating Shard Files...")
city_list = sorted(list(database.keys()))

# Save cities index
with open(f'{output_dir}/cities.json', 'w', encoding='utf-8') as f:
    json.dump(city_list, f, ensure_ascii=False)

# Save each city as a separate JSON file
count = 0
for city, streets in database.items():
    # Create a safe filename (e.g., "Tel Aviv.json")
    safe_filename = city.replace('"', '').replace('/', '-').replace('\\', '-').strip()
    if not safe_filename: continue
    
    with open(f'{output_dir}/{safe_filename}.json', 'w', encoding='utf-8') as f:
        json.dump(streets, f, ensure_ascii=False, separators=(',', ':'))
    count += 1

print(f"Success! Created {count} city files in '{output_dir}/'.")
print("Please upload the entire 'data' folder to your GitHub repository.")
