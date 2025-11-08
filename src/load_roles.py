import os, glob, re, unicodedata
from dotenv import load_dotenv
from supabase import create_client

# ---- config ----
TEXT_DIR = "text"
SYNC_DELETE = False  # set True to remove table rows not present in files
ALLOWED_ROLES = {"meal", "accommodation", "attraction"}

def norm_token(s: str) -> str:
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def read_list(path):
    with open(path, "r", encoding="utf-8") as f:
        return [norm_token(ln) for ln in f if ln.strip()]

def rows_for_file(path, role):
    if role not in ALLOWED_ROLES:
        raise ValueError(f"Invalid role: {role}")
    return [{"category": tok, "role": role} for tok in read_list(path)]

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

rows = []

# meal
meal_fp = os.path.join(TEXT_DIR, "meal.txt")
if os.path.exists(meal_fp):
    rows += rows_for_file(meal_fp, "meal")

# accommodation (handle both spellings if you keep one around)
acc_fp = os.path.join(TEXT_DIR, "accommodation.txt")
if os.path.exists(acc_fp):
    rows += rows_for_file(acc_fp, "accommodation")
acc2_fp = os.path.join(TEXT_DIR, "accomodation.txt")
if os.path.exists(acc2_fp):
    rows += rows_for_file(acc2_fp, "accommodation")

# attractions/*.txt + unique.txt (all mapped as "attraction")
attr_dir = os.path.join(TEXT_DIR, "attractions")
if os.path.isdir(attr_dir):
    for fp in glob.glob(os.path.join(attr_dir, "*.txt")):
        rows += rows_for_file(fp, "attraction")

uniq_fp = os.path.join(TEXT_DIR, "unique.txt")
if os.path.exists(uniq_fp):
    rows += rows_for_file(uniq_fp, "attraction")

# de-dup exact (category,role)
pairs = sorted({(r["category"], r["role"]) for r in rows})
payload = [{"category": c, "role": r} for (c, r) in pairs]
print("to upsert:", len(payload))

# ---- upsert in chunks ----
for i in range(0, len(payload), 1000):
    chunk = payload[i:i+1000]
    sb.table("category_role_map").upsert(
        chunk, on_conflict="category,role"
    ).execute()

print("✅ done")

# ---- optional: sync-delete stale pairs not present in files ----
if SYNC_DELETE:
    # fetch current table
    cur = sb.table("category_role_map").select("category,role").execute().data
    cur_pairs = {(r["category"], r["role"]) for r in cur}
    desired = set(pairs)
    stale = cur_pairs - desired
    if stale:
        print("removing stale rows:", len(stale))
        # delete in small batches
        stale_list = [{"category": c, "role": r} for (c, r) in stale]
        for i in range(0, len(stale_list), 200):
            batch = stale_list[i:i+200]
            # Supabase delete with composite key
            # do per (category,role) OR chain
            q = sb.table("category_role_map").delete()
            for j, item in enumerate(batch):
                if j == 0:
                    q = q.eq("category", item["category"]).eq("role", item["role"])
                else:
                    q = q.or_(f"(category.eq.{item['category']},role.eq.{item['role']})")
            q.execute()
    print("sync-delete done (if enabled)")

print("roles upload complete")
