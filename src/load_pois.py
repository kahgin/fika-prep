import os
import ast
import json
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

df = pd.read_csv('output/poi.csv')

def safe_parse(value):
    if pd.isna(value) or value == '{}':
        return None
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except:
            return value
    return value

def to_list_tokens(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return []
    if isinstance(v, list): return [str(x).strip().lower() for x in v if str(x).strip()]
    if isinstance(v, str):
        s=v.strip()
        for parser in (json.loads, ast.literal_eval):
            try:
                vv=parser(s)
                if isinstance(vv, list): return [str(x).strip().lower() for x in vv if str(x).strip()]
            except: pass
        sep = "," if "," in s else "|"
        if sep in s: return [t.strip().lower() for t in s.replace("|",",").split(",") if t.strip()]
        return [s.lower()]
    return [str(v).strip().lower()]

def prepare_row(row):
    return {
        'google_map_link': row['link'],
        'name': row['name'],
        'categories': to_list_tokens(row.get('categories')),
        'address': row['address'] if pd.notna(row['address']) else None,
        'timezone': row['timezone'] if pd.notna(row['timezone']) else None,
        'open_hours': safe_parse(row['open_hours']),
        'website': row['website'] if pd.notna(row['website']) else None,
        'phone': row['phone'] if pd.notna(row['phone']) else None,
        'review_count': int(row['review_count']) if pd.notna(row['review_count']) else None,
        'review_rating': float(row['review_rating']) if pd.notna(row['review_rating']) else None,
        # 'reviews_per_rating': safe_parse(row['reviews_per_rating']),
        'latitude': float(row['latitude']) if pd.notna(row['latitude']) else None,
        'longitude': float(row['longitude']) if pd.notna(row['longitude']) else None,
        'descriptions': row['descriptions'] if pd.notna(row['descriptions']) else None,
        'price_level': float(row['price_level']) if pd.notna(row['price_level']) else None,
        'images': safe_parse(row['images']),
        # 'videos': safe_parse(row['videos']),
        'complete_address': safe_parse(row['complete_address']),
        'about': safe_parse(row['about']),

        # Boolean
        'kids_friendly': bool(row['kids_friendly']) if pd.notna(row['kids_friendly']) else False,
        'pets_friendly': bool(row['pets_friendly']) if pd.notna(row['pets_friendly']) else False,
        'wheelchair_rental': bool(row['wheelchair_rental']) if pd.notna(row['wheelchair_rental']) else False,
        'wheelchair_accessible_car_park': bool(row['wheelchair_accessible_car_park']) if pd.notna(row['wheelchair_accessible_car_park']) else False,
        'wheelchair_accessible_entrance': bool(row['wheelchair_accessible_entrance']) if pd.notna(row['wheelchair_accessible_entrance']) else False,
        'wheelchair_accessible_seating': bool(row['wheelchair_accessible_seating']) if pd.notna(row['wheelchair_accessible_seating']) else False,
        'wheelchair_accessible_toilet': bool(row['wheelchair_accessible_toilet']) if pd.notna(row['wheelchair_accessible_toilet']) else False,
        'halal_food': bool(row['halal_food']) if pd.notna(row['halal_food']) else False,
        'vegan_options': bool(row['vegan_options']) if pd.notna(row['vegan_options']) else False,
        'vegetarian_options': bool(row['vegetarian_options']) if pd.notna(row['vegetarian_options']) else False,
        'reservations_required': bool(row['reservations_required']) if pd.notna(row['reservations_required']) else False,
        # 'hiking': bool(row['hiking']) if pd.notna(row['hiking']) else False,
        # 'cycling': bool(row['cycling']) if pd.notna(row['cycling']) else False,
    }

BATCH_SIZE = 1000
total_rows = len(df)

for i in range(0, total_rows, BATCH_SIZE):
    batch = df.iloc[i:i+BATCH_SIZE]
    data = [prepare_row(row) for _, row in batch.iterrows()]
    try:
        supabase.table('pois').upsert(data, on_conflict='google_map_link').execute()
        print(f"Upserted batch {i//BATCH_SIZE + 1}: {len(data)} rows")
    except Exception as e:
        print(f"Error on batch {i//BATCH_SIZE + 1}: {e}")

print(f"Upload complete! Total rows: {total_rows}")
