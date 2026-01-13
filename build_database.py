import pandas as pd
import json
import os
import shutil

# --- CONFIGURATION ---
installed_file = 'iec_sources/installed_meters.csv'
planned_file = 'iec_sources/annual_plan_26.csv'
output_dir = 'data'

# Ensure output directory exists
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
os.makedirs(output_dir)

database = {}

def clean_key(val):
    return str(val).strip()

print("1. Processing Installed Meters...")
try:
    df = pd.read_csv(installed_file)
    # Find columns dynamically
    city_col = [c for c in df.columns if 'עיר' in c][0]
    street_col = [c for c in df.columns if 'רחוב' in c][0]
    num_col = [c for c in df.columns if 'בית' in c][0]

    for _, row in df.iterrows():
        city = clean_key(row[city_col])
        street = clean_key(row[street_col])
        num = clean_key(row[num_col])
        
        if not city or not street: continue
        if city not in database: database[city] = {}
        if street not in database[city]: database[city][street] = {'i': [], 'p': {}}
        
        database[city][street]['i'].append(num)
except Exception as e:
    print(f"Error: {e}")

print("2. Processing Planned 2026 Meters...")
try:
    df = pd.read_csv(planned_file)
    city_col = [c for c in df.columns if 'עיר' in c][0]
    street_col = [c for c in df.columns if 'רחוב' in c][0]
    num_col = [c for c in df.columns if 'בית' in c][0]
    quart_col = [c for c in df.columns if 'רבעון' in c or 'Quarter' in c][0]

    for _, row in df.iterrows():
        city = clean_key(row[city_col])
        street = clean_key(row[street_col])
        num = clean_key(row[num_col])
        quarter = clean_key(row[quart_col])
        
        if not city or not street: continue
        if city not in database: database[city] = {}
        if street not in database[city]: database[city][street] = {'i': [], 'p': {}}
        
        database[city][street]['p'][num] = quarter
except Exception as e:
    print(f"Error: {e}")

print("3. Generating Shard Files...")
city_list = sorted(list(database.keys()))

# Save cities index
with open(f'{output_dir}/cities.json', 'w', encoding='utf-8') as f:
    json.dump(city_list, f, ensure_ascii=False)

# Save each city as a separate JSON file
for city, streets in database.items():
    safe_filename = city.replace('"', '').replace('/', '-').replace('\\', '-')
    with open(f'{output_dir}/{safe_filename}.json', 'w', encoding='utf-8') as f:
        json.dump(streets, f, ensure_ascii=False, separators=(',', ':'))

print(f"Success! Created {len(city_list)} city files in '{output_dir}/'.")
print("Upload the 'data' folder to your GitHub repo.")
