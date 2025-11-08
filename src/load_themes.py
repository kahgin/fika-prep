import os, glob
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

THEMES_DIR = "text/attractions"

THEME_KEYS = [
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

def read_tokens(path):
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip().lower() for ln in f if ln.strip()]

rows = []
for key in THEME_KEYS:
    fp = os.path.join(THEMES_DIR, f"{key}.txt")
    if not os.path.exists(fp):
        print(f"SKIP: {fp} not found")
        continue
    cats = read_tokens(fp)
    rows.extend({"theme": key, "category": c} for c in cats)

# de-dup
pairs = {(r["theme"], r["category"]) for r in rows}
rows = [{"theme": t, "category": c} for (t, c) in sorted(pairs)]
print("to upsert:", len(rows))

# upsert in chunks on composite key (theme, category)
for i in range(0, len(rows), 1000):
    chunk = rows[i:i+1000]
    sb.table("theme_category_map").upsert(
        chunk, on_conflict="theme,category"
    ).execute()

print("✅ done")
