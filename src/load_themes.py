import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

CANDIDATES = [
    "text/attractions",
]
THEMES_DIR = next((p for p in CANDIDATES if os.path.isdir(p)), None)
if THEMES_DIR is None:
    raise SystemExit("No themes directory found. Create one of: " + ", ".join(CANDIDATES))

EXPECTED_KEYS = [
    "religious",
    "adventure",
    "art_craft",
    "family",
    "nature",
    "nightlife",
    "relax",
    "shopping",
    "cultural_history",
    "food_and_drink",
]

DISPLAY_NAME = {
    "religious": "Religious Sites",
    "adventure": "Adventure",
    "art_craft": "Arts & Crafts",
    "family": "Family Attractions",
    "nature": "Nature & Parks",
    "nightlife": "Nightlife",
    "relax": "Relax & Leisure",
    "shopping": "Shopping",
    "cultural_history": "Cultural & History",
    "food_and_drink": "Food & Culinary",
}

def read_tokens(path):
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip().lower() for ln in f if ln.strip()]

payloads = []
for key in EXPECTED_KEYS:
    fp = os.path.join(THEMES_DIR, f"{key}.txt")
    if not os.path.exists(fp):
        print(f"SKIP: {fp} not found")
        continue
    cats = read_tokens(fp)
    payloads.append({
        "key": key,
        "display_name": DISPLAY_NAME.get(key, key.replace("_"," ").title()),
        "category_whitelist": cats,
        "category_weights": None,
    })

if not payloads:
    raise SystemExit("No theme files loaded. Check folder and filenames.")

sb.table("themes").upsert(payloads, on_conflict="key").execute()
print(f"Upserted/updated {len(payloads)} themes")
