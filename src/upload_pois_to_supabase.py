import os
import ast
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# Initialize Supabase
supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Read CSV
df = pd.read_csv('output/poi.csv')

def prepare_row(row):
    """Convert pandas row to Supabase-ready dict"""
    
    # Parse string representations to actual objects
    def safe_parse(value):
        if pd.isna(value) or value == '{}':
            return None
        if isinstance(value, str):
            try:
                return ast.literal_eval(value)
            except:
                return value
        return value
    
    def parse_categories(value):
        if pd.isna(value):
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [v.strip() for v in value.split(",") if v.strip()]
        return []
    
    return {
        'google_map_link': row['link'],
        'name': row['name'],
        'categories': parse_categories(row['categories']),  # Convert to array
        'address': row['address'] if pd.notna(row['address']) else None,
        'open_hours': safe_parse(row['open_hours']),
        'website': row['website'] if pd.notna(row['website']) else None,
        'phone': row['phone'] if pd.notna(row['phone']) else None,
        'review_count': int(row['review_count']) if pd.notna(row['review_count']) else None,
        'review_rating': float(row['review_rating']) if pd.notna(row['review_rating']) else None,
        'reviews_per_rating': safe_parse(row['reviews_per_rating']),
        'latitude': float(row['latitude']) if pd.notna(row['latitude']) else None,
        'longitude': float(row['longitude']) if pd.notna(row['longitude']) else None,
        'descriptions': row['descriptions'] if pd.notna(row['descriptions']) else None,
        'price_level': float(row['price_level']) if pd.notna(row['price_level']) else None,
        'images': safe_parse(row['images']),
        'videos': safe_parse(row['videos']),
        'complete_address': safe_parse(row['complete_address']),
        'about': safe_parse(row['about']),
        
        # Booleans
        'kids_friendly': bool(row['kids_friendly']) if pd.notna(row['kids_friendly']) else False,
        'dogs_friendly': bool(row['dogs_friendly']) if pd.notna(row['dogs_friendly']) else False,
        'wheelchair_rental': bool(row['wheelchair_rental']) if pd.notna(row['wheelchair_rental']) else False,
        'wheelchair_accessible_car_park': bool(row['wheelchair_accessible_car_park']) if pd.notna(row['wheelchair_accessible_car_park']) else False,
        'wheelchair_accessible_entrance': bool(row['wheelchair_accessible_entrance']) if pd.notna(row['wheelchair_accessible_entrance']) else False,
        'wheelchair_accessible_seating': bool(row['wheelchair_accessible_seating']) if pd.notna(row['wheelchair_accessible_seating']) else False,
        'wheelchair_accessible_toilet': bool(row['wheelchair_accessible_toilet']) if pd.notna(row['wheelchair_accessible_toilet']) else False,
        'halal_food': bool(row['halal_food']) if pd.notna(row['halal_food']) else False,
        'vegan_options': bool(row['vegan_options']) if pd.notna(row['vegan_options']) else False,
        'vegetarian_options': bool(row['vegetarian_options']) if pd.notna(row['vegetarian_options']) else False,
        'reservations_required': bool(row['reservations_required']) if pd.notna(row['reservations_required']) else False,
        'hiking': bool(row['hiking']) if pd.notna(row['hiking']) else False,
        'cycling': bool(row['cycling']) if pd.notna(row['cycling']) else False,
    }

# Batch upload (1000 rows at a time for efficiency)
BATCH_SIZE = 1000
total_rows = len(df)

for i in range(0, total_rows, BATCH_SIZE):
    batch = df.iloc[i:i+BATCH_SIZE]
    data = [prepare_row(row) for _, row in batch.iterrows()]
    
    try:
        response = supabase.table('pois').insert(data).execute()
        print(f"Uploaded batch {i//BATCH_SIZE + 1}: {len(data)} rows")
    except Exception as e:
        print(f"Error uploading batch {i//BATCH_SIZE + 1}: {e}")
        # Log failed rows for debugging
        for idx, row_data in enumerate(data):
            try:
                supabase.table('pois').insert(row_data).execute()
            except Exception as row_error:
                print(f"Failed row {i+idx}: {row_error}")

print(f"Upload complete! Total rows: {total_rows}")