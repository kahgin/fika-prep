import os
import re
import ast
import json
import glob
import unicodedata
import pandas as pd
from pathlib import Path

pd.set_option('future.no_silent_downcasting', True)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)

os.chdir('/home/kahgin/fika/fika-prep')

# CONSTANTS

INPUT_DIR = 'data/map'
OUTPUT_DIR = 'output'
TEXT_DIR = 'data/text'

_UNICODE_REPLACEMENTS = {'\u202f': ' ', '\u2013': '-', '\u0026': '&'}
_PRICE_SYMBOLS = {'$': 1, '$$': 2, '$$$': 3, '$$$$': 4}
_STREET_VIEW_KEYWORDS = ['street view', '360', 'streetview']
_ABOUT_REMOVE_CATEGORIES = [
    'Atmosphere', 'Amenities', 'Dining options', 'From the business',
    'Getting here', 'Offerings', 'Parking', 'Payments', 'Pets',
    'Popular for', 'Recycling', 'Service options'
]
_DROP_COLUMNS = [
    'input_id', 'popular_times', 'videos', 'reviews_per_rating',
    'cid', 'status', 'reviews_link', 'thumbnail', 'data_id', 'reservations',
    'order_online', 'menu', 'owner', 'user_reviews', 'user_reviews_extended', 'emails'
]
# _DROP_COLUMNS_EXTENDED = ['plus_code', 'timezone', 'complete_address']

# CACHED REGEX PATTERNS
_WHITESPACE_PATTERN = re.compile(r"\s+")
_PHONE_CLEANUP_PATTERN = re.compile(r'[\s\-\(\)\.\/]')
_PRICE_DIGITS_PATTERN = re.compile(r'\d+')

# TEXT NORMALIZATION UTILITIES
def norm_token(s: str) -> str:
    """Normalize text to slug format"""
    s = _WHITESPACE_PATTERN.sub(" ", s.strip().lower().replace("&", " and "))
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s

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
    
    digits = _PHONE_CLEANUP_PATTERN.sub('', phone_str)
    
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

def map_price(price):
    """Map price strings to 1-4 scale"""
    if pd.isna(price):
        return None
    price = str(price).strip()
    if price in _PRICE_SYMBOLS:
        return _PRICE_SYMBOLS[price]
    nums = [int(n) for n in _PRICE_DIGITS_PATTERN.findall(price)]
    if not nums:
        return None
    mid = sum(nums) / len(nums)
    if mid < 20: return 1
    if mid <= 50: return 2
    if mid <= 100: return 3
    return 4

# CATEGORY PROCESSING UTILITIES

def categories_to_tokens(val):
    """Parse categories from various formats to normalized tokens"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    
    if isinstance(val, list):
        items = [str(x) for x in val if str(x).strip()]
    elif isinstance(val, str):
        val = val.strip()
        try:
            parsed = json.loads(val)
            items = [str(x) for x in parsed if str(x).strip()] if isinstance(parsed, list) else [p.strip() for p in val.split(",") if p.strip()]
        except Exception:
            items = [p.strip() for p in val.split(",") if p.strip()]
    else:
        items = [str(val).strip()] if str(val).strip() else []
    
    return [t for x in items if (t := norm_token(x))]

def categories_json_to_list(val) -> list[str]:
    """Parse JSON list string to Python list"""
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            v = json.loads(val)
            if isinstance(v, list):
                return v
        except Exception:
            pass
    return []

def normalize_categories_column(df: pd.DataFrame, src="categories", dst="categories"):
    """Convert categories column to JSON array of normalized tokens"""
    df[dst] = df[src].apply(categories_to_tokens).apply(json.dumps)
    return df

def deprioritize_category(row, keyword):
    """Move category to end of list (deprioritize)"""
    toks = categories_json_to_list(row.get("categories"))
    key = norm_token(keyword)
    if key in toks:
        toks = [t for t in toks if t != key] + [key]
    return json.dumps(toks)

def filter_exclude_categories(df, exclude_file='../text/exclude.txt'):
    """
    Filter excluded categories with these rules:
    1. Remove entire row if excluded category is first
    2. If excluded exists but not first, drop the category
    3. If row becomes empty after dropping, remove the row
    4. If excluded exists with tourist attraction, keep both
    """
    try:
        with open(exclude_file, 'r', encoding='utf-8') as f:
            exclude = {norm_token(line) for line in f if line.strip()}
    except FileNotFoundError:
        return df

    if not exclude:
        return df

    def _process_row(val):
        toks = categories_json_to_list(val)
        if not toks:
            return json.dumps([])
        
        # Rule 4: If tourist attraction exists, keep all categories (including excluded ones)
        if "tourist attraction" in toks:
            return json.dumps(toks)
        
        # Rule 1: If first category is excluded, mark for removal
        if toks[0] in exclude:
            return None
        
        # Rule 2: Remove excluded categories from non-first positions
        filtered_toks = [t for t in toks if t not in exclude]
        
        # Rule 3: If list becomes empty after filtering, mark for removal
        if not filtered_toks:
            return None
        
        return json.dumps(filtered_toks)

    df['categories'] = df['categories'].apply(_process_row)
    df = df[df['categories'].notna()].reset_index(drop=True)
    return df

# IMAGE PROCESSING UTILITIES

def process_images(images):
    """Combined image processing: remove street view, extract URLs, scale resolution"""
    if not isinstance(images, (list, str)):
        return images
    
    # Step 1: Remove street view
    try:
        image_json = json.loads(images) if isinstance(images, str) else images
        filtered = [
            img for img in image_json
            if not any(kw in str(img.get('title', '')).lower() for kw in _STREET_VIEW_KEYWORDS)
            and 'streetview' not in str(img.get('image', '')).lower()
        ]
    except (json.JSONDecodeError, TypeError):
        return images
    
    # Step 2: Extract image URLs
    if any(isinstance(item, dict) for item in filtered):
        urls = [
            item["image"]
            for item in filtered
            if isinstance(item, dict) and isinstance(item.get("image"), str)
        ]
    elif any(isinstance(item, str) for item in filtered):
        urls = filtered
    else:
        return filtered
    
    # Step 3: Scale resolution
    return [link.split("=w")[0] for link in urls if isinstance(link, str)]

# ABOUT FIELD UTILITIES

def remove_about(row, category_name):
    """Remove category block from 'about' JSON"""
    if pd.isna(row) or pd.isna(category_name):
        return row
    data = json.loads(row) if isinstance(row, str) else row
    filtered = [cat for cat in data if cat.get('name') != category_name]
    return json.dumps(filtered) if isinstance(row, str) else filtered

def update_flag_unified(row, flag_col, source, keywords=None, target_categories=None, about_col='about'):
    """Unified flag update from either 'about' options or categories"""
    if row.get(flag_col, False):
        return True
    
    if source == 'about' and keywords:
        about_data = row.get(about_col)
        if pd.isna(about_data):
            return False
        
        if isinstance(about_data, str):
            try:
                about_data = json.loads(about_data)
            except json.JSONDecodeError:
                return False
        
        keywords_lower = [kw.lower() for kw in keywords]
        for cat in about_data:
            for opt in cat.get('options', []):
                if opt.get('enabled', False):
                    opt_name = opt.get('name', '').lower()
                    if any(kw in opt_name for kw in keywords_lower):
                        return True
    
    elif source == 'categories' and target_categories:
        cats = set(categories_json_to_list(row.get("categories")))
        targets = {norm_token(t) for t in target_categories}
        return bool(cats & targets)
    
    return False

# FILE I/O UTILITIES

def to_list(s):
    """Convert string/list to Python list"""
    if isinstance(s, list):
        return s
    if not s or not isinstance(s, str):
        return []
    try:
        return ast.literal_eval(s)
    except Exception:
        return []

def read_set(path: Path) -> set[str]:
    """Read text file into set"""
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}

def write_set(path: Path, items: set[str]) -> None:
    """Write set to text file"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(sorted(items)) + ("\n" if items else ""), encoding="utf-8")

def to_csv(df, filename):
    """Save dataframe to CSV"""
    if not df.empty:
        df.to_csv(filename, index=False)

def save_categories(df, exclude_keyword=None, filename='../text/categories.txt'):
    """Export unique categories to text file"""
    tokens = []
    for val in df['categories'].dropna():
        tokens.extend(categories_json_to_list(val))

    unique = sorted(set(tokens))

    if exclude_keyword:
        ex_kw = [kw.lower() for kw in exclude_keyword]
        unique = [c for c in unique if all(kw not in c.lower() for kw in ex_kw)]

    with open(filename, 'w', encoding='utf-8') as f:
        for category in unique:
            f.write(f"{category}\n")

def save_about_field(df, filename=os.path.join(TEXT_DIR, 'about_field.txt')):
    """Export 'about' field structure to text file"""
    from collections import defaultdict
    grouped = defaultdict(set)
    
    for about_data in df["about"].dropna():
        if isinstance(about_data, str):
            try:
                about_data = json.loads(about_data)
            except json.JSONDecodeError:
                continue
        for cat in about_data:
            cname = cat.get("name")
            if not cname:
                continue
            for opt in cat.get("options", []):
                if opt.get("enabled"):
                    grouped[cname].add(opt.get("name", "").strip())

    with open(filename, "w", encoding="utf-8") as f:
        for cname in sorted(grouped):
            f.write(f"{cname}\n")
            for oname in sorted(grouped[cname]):
                f.write(f"- {oname}\n")
            f.write("\n")

# DATAFRAME OPERATIONS
def combine_dataframes(dfs):
    """Concat dataframes, dedup by name, print dup count"""
    combined = pd.concat(dfs, ignore_index=True)
    num_duplicates = combined['name'].duplicated().sum()
    combined = combined.drop_duplicates(subset=['name'])
    print(f"duplicate rows: {num_duplicates}")
    return combined

def clean_data(filename):
    """Main data cleaning pipeline"""
    df = pd.read_csv(filename, low_memory=False)

    # Replace unicode variants
    for old, new in _UNICODE_REPLACEMENTS.items():
        df = df.replace(old, new, regex=True)


    # Filter rows and apply quality thresholds
    df = df[
        (df['complete_address'].astype(str).str.contains('"country":"SG"', na=False) | df['complete_address'].astype(str).str.contains('"country":"MY"', na=False)) &
        (df['review_count'].astype(int) >= 50) &
        (df['review_rating'].astype(float) >= 2.5)
    ]

    df.drop(columns=_DROP_COLUMNS, inplace=True, errors='ignore')

    # PRESAVE: Save filtered data to reduce reload time
    # df.to_csv(filename, index=False)

    df.rename(columns={'title': 'name'}, errors='ignore', inplace=True)

    # Rename, deduplicate, normalize
    df = normalize_categories_column(df, src='categories', dst='categories')
    df['phone'] = df['phone'].apply(normalize_phone_e164)
    # df.drop(columns=_DROP_COLUMNS_EXTENDED, inplace=True, errors='ignore')

    return df

# MAIN PROCESSING FUNCTIONS
def process_poi_data():
    """Process POI data from CSV files"""
    # Read and clean all CSV files
    csv_files = glob.glob(os.path.join(INPUT_DIR, "**", "*.csv"), recursive=True)
    dataframes = [clean_data(file) for file in csv_files]
    pois = combine_dataframes(dataframes)

    # Map price range to 1-4 scale
    pois['price_range'] = pois['price_range'].apply(map_price)
    pois = pois.rename(columns={"price_range": "price_level"})

    save_categories(pois, filename=os.path.join(TEXT_DIR, 'categories.txt'))
    save_about_field(pois, filename=os.path.join(TEXT_DIR, 'about_field.txt'))

    # Deprioritize 'Tourist attraction' category BEFORE filtering
    pois['categories'] = pois.apply(deprioritize_category, axis=1, keyword='Tourist attraction')

    # Filter excluded categories
    pois = filter_exclude_categories(pois, exclude_file=os.path.join(TEXT_DIR, 'exclude.txt'))

    # Clean images: remove street view, extract URLs, scale resolution (Refactor #4)
    pois['images'] = pois['images'].apply(process_images)

    # Unified flag update configuration (Refactor #1 & #2)
    flag_configs = [
        ('kids_friendly', 'about', ['Good for kids'], None),
        ('pets_friendly', 'about', ['Dogs allowed', 'Dogs allowed inside', 'Dogs allowed outside'], None),
        ('wheelchair_rental', 'about', ['Wheelchair rental'], None),
        ('wheelchair_accessible_car_park', 'about', ['Wheelchair-accessible car park'], None),
        ('wheelchair_accessible_entrance', 'about', ['Wheelchair-accessible entrance'], None),
        ('wheelchair_accessible_seating', 'about', ['Wheelchair-accessible seating'], None),
        ('wheelchair_accessible_toilet', 'about', ['Wheelchair-accessible toilet'], None),
        ('halal_food', 'about', ['Halal food'], None),
        ('vegan_options', 'about', ['Vegan options'], None),
        ('vegetarian_options', 'about', ['Vegetarian options'], None),
        ('reservations_required', 'about', ['Reservations required'], None),
        # ('hiking', 'about', ['Hiking', 'Point-to-point trail', 'Trail difficulty'], None),
        # ('cycling', 'about', ['Cycling'], None),
        ('halal_food', 'categories', None, ['Halal restaurant']),
        ('vegetarian_options', 'categories', None, ['Vegetarian restaurant', 'Vegetarian cafe and deli']),
        ('vegan_options', 'categories', None, ['Vegan restaurant']),
        ('pets_friendly', 'categories', None, ['Cat cafe', 'Dog cafe']),
    ]

    for flag_col, source, keywords, target_categories in flag_configs:
        pois[flag_col] = pois.apply(
            update_flag_unified, 
            axis=1, 
            flag_col=flag_col, 
            source=source, 
            keywords=keywords, 
            target_categories=target_categories
        )

    # Remove unnecessary 'about' categories
    for cat_name in _ABOUT_REMOVE_CATEGORIES:
        pois['about'] = pois['about'].apply(remove_about, category_name=cat_name)

    # to_csv(pois, f"{OUTPUT_DIR}/poi.csv")
    return pois

def integrate_michelin(pois, michelin_path=os.path.join(OUTPUT_DIR, "michelin.csv")):
    """Integrate Michelin data with POI data (Refactor #3)"""
    # Load Michelin data
    michelin = pd.read_csv(michelin_path, low_memory=False)
    michelin["phone"] = michelin["phone"].apply(normalize_phone_e164)
    michelin["price"] = michelin["price"].apply(map_price)
    michelin["images"] = michelin["images"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else [])

    # Create phone lookup
    michelin_by_phone = michelin.dropna(subset=["phone"]).drop_duplicates(subset=["phone"]).set_index("phone")

    # matched_count = sum(1 for phone in pois["phone"].dropna() if phone in michelin_by_phone.index)
    # total_michelin = len(michelin_by_phone)
    # print(f"Michelin match: {matched_count}/{total_michelin} ({matched_count/total_michelin:.1%})")

    # Fill missing price_level from Michelin data
    def get_michelin_field(row, field, fallback_col):
        phone = row.get("phone")
        if pd.notna(phone) and phone in michelin_by_phone.index:
            return michelin_by_phone.loc[phone, field]
        return row.get(fallback_col)

    pois["price_level"] = pois.apply(get_michelin_field, axis=1, field="price", fallback_col="price_level")
    pois["descriptions"] = pois.apply(get_michelin_field, axis=1, field="description", fallback_col="descriptions")

    # Merge Michelin images with existing images
    def merge_images(row):
        existing = to_list(row["images"])
        phone = row.get("phone")
        michelin_imgs = michelin_by_phone.loc[phone, "images"] if pd.notna(phone) and phone in michelin_by_phone.index else []
        return michelin_imgs + existing

    pois["images"] = pois.apply(merge_images, axis=1)

    return pois

def manage_categories():
    """Category management: compute unique categories not in any group"""
    TEXT_DIR_PATH = Path("text")
    ATTRACTIONS_DIR = TEXT_DIR_PATH / "attractions"
    ATTRACTIONS_DIR.mkdir(parents=True, exist_ok=True)

    # Load all categories
    categories = read_set(TEXT_DIR_PATH / "categories.txt")

    # Load root category groups
    root_files = {
        "exclude": TEXT_DIR_PATH / "exclude.txt",
        "meal": TEXT_DIR_PATH / "meal.txt",
        "accommodation": TEXT_DIR_PATH / "accommodation.txt",
    }

    groups: dict[str, set[str]] = {name: read_set(path) for name, path in root_files.items()}

    # Load all attraction category files
    for p in ATTRACTIONS_DIR.glob("*.txt"):
        groups[p.stem] = read_set(p)

    # Constrain to known categories
    for k in groups:
        groups[k] &= categories

    # Special rule: family excludes nature and cultural_history
    # if "family" in groups:
        # groups["family"] -= groups.get("nature", set()) | groups.get("cultural_history", set())

    # Compute unique categories
    filter_categories = set().union(*groups.values()) if groups else set()
    unique = categories - filter_categories

    # Write outputs
    write_set(TEXT_DIR_PATH / "unique.txt", unique)

    for name, items in groups.items():
        out_path = root_files[name] if name in root_files else ATTRACTIONS_DIR / f"{name}.txt"
        write_set(out_path, items)

def main():
    """Main execution entry point"""
    pois = process_poi_data()
    pois = integrate_michelin(pois)
    to_csv(pois, os.path.join(OUTPUT_DIR, "poi.csv"))
    manage_categories()

if __name__ == "__main__":
    main()