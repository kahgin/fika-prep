import os, glob
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

TEXT_DIR = "text/"

def read_tokens(path):
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip().lower() for ln in f if ln.strip()]

def rows_for_file(path, role):
    return [{"category": tok, "role": role} for tok in read_tokens(path)]

rows = []

# meal.txt
meal_file = os.path.join(TEXT_DIR, "meal.txt")
if os.path.exists(meal_file):
    rows += rows_for_file(meal_file, "meal")

# accommodation.txt
acc = os.path.join(TEXT_DIR, "accommodation.txt")
if os.path.exists(acc):
    rows += rows_for_file(acc, "accommodation")

# attractions/*.txt + unique.txt
attr_folder = os.path.join(TEXT_DIR, "attractions")
if os.path.isdir(attr_folder):
    for fp in glob.glob(os.path.join(attr_folder, "*.txt")):
        rows += rows_for_file(fp, "attraction")

uniq = os.path.join(TEXT_DIR, "unique.txt")
if os.path.exists(uniq):
    rows += rows_for_file(uniq, "attraction")

# de-dup (category, role)
pairs = {(r["category"], r["role"]) for r in rows}
dedup = [{"category": c, "role": r} for (c, r) in sorted(pairs)]
print("to upsert:", len(dedup))

# upsert in chunks
for i in range(0, len(dedup), 1000):
    chunk = dedup[i:i+1000]
    sb.table("category_role_map").upsert(chunk, on_conflict="category,role").execute()

print("done")
