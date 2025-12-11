import glob
import os
import re
import pandas as pd

os.chdir('/home/kahgin/fika/fika-prep')
INPUT_DIR = "data/michelin"
OUTPUT_DIR = "output/"

def str_to_list(val):
    if pd.isna(val):
        return []
    if isinstance(val, list):
        return val
    return [img.strip() for img in val.split(",") if img.strip()]

def normalize_phone_e164(phone):
    """
    Normalize phone to E.164 format: +[country_code][number]
    """
    if pd.isna(phone):
        return None
    
    phone_str = str(phone).strip()
    if not phone_str or phone_str.lower() in ('nan', 'none', ''):
        return None
    
    if phone_str.endswith('.0'):
        phone_str = phone_str[:-2]
    
    digits = re.sub(r'[\s\-\(\)]', '', phone_str)
    
    if digits.startswith('+'):
        return digits
    
    if digits.startswith('0'):
        return '+60' + digits[1:]
    
    if len(digits) == 8 and digits[0] in '689':
        return '+65' + digits
    
    if len(digits) in (9, 10) and digits[0] == '1':
        return '+60' + digits
    
    if digits.startswith('60') or digits.startswith('65'):
        return '+' + digits
    
    if digits.isdigit():
        return '+' + digits
    
    return None

def clean_dfs(michelin):
    michelin.drop(columns=['ID'], inplace=True, errors='ignore')
    michelin.columns = [col.lower() for col in michelin.columns]
    rename_map = {'websiteurl': 'website', 'phonenumber': 'phone', 'pricerange': 'price'}
    existing_renames = {k: v for k, v in rename_map.items() if k in michelin.columns}
    michelin.rename(columns=existing_renames, inplace=True)
        
    michelin['phone'] = michelin['phone'].apply(normalize_phone_e164)
    michelin["images"] = michelin["images"].apply(str_to_list)

def combine_dataframes(dfs):
    """Concat dataframes, dedup by name, print dup count"""
    combined = pd.concat(dfs, ignore_index=True)
    num_duplicates = combined['name'].duplicated().sum()
    combined = combined.drop_duplicates(subset=['name'])
    print(f"duplicate rows: {num_duplicates}")
    return combined

michelin = glob.glob(os.path.join(INPUT_DIR, "michelin*.csv"))
michelin = [pd.read_csv(file) for file in michelin]
michelin = [clean_dfs(df) or df for df in michelin]
michelin = combine_dataframes(michelin)
michelin.to_csv(os.path.join(OUTPUT_DIR, "michelin.csv"), index=False)