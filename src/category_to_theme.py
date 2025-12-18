import os
import json
import yaml
from pathlib import Path

from tqdm import tqdm
from dotenv import load_dotenv
import google.generativeai as genai

# --- Environment / Gemini setup ---

ERROR_LOG = Path("text/classify_errors.log")

# Load .env and read API key
load_dotenv()
API_KEY = os.getenv("GOOGLE_AI_STUDIO_KEY")
if not API_KEY:
    raise RuntimeError("GOOGLE_AI_STUDIO_KEY not set in environment (.env)")

genai.configure(api_key=API_KEY)

# Cheapest / fastest general text model on the Gemini API
MODEL_NAME = "gemini-2.5-flash"
model = genai.GenerativeModel(MODEL_NAME)


# --- Config ---

INPUT_FILE = "text/categories.txt"
OUTPUT_DIR = Path("text")
BATCH_SIZE = 10
MAX_RETRIES = 3

# Output buckets (fixed)
ATTRACTIONS = [
    "food_culinary",
    "adventure",
    "art_museums",
    "family",
    "cultural_history",
    "nature",
    "nightlife",
    "relax",
    "religious_sites",
    "shopping",
]
ALL_CATEGORY_KEYS = ["meal", "accommodation"] + [f"attractions/{k}" for k in ATTRACTIONS]


def norm(s: str) -> str:
    import re

    return re.sub(r"\s+", " ", s.strip().lower())


def slugify(s: str) -> str:
    import re

    s = norm(s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def load_checkpoint(path: Path):
    data = {}
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    j = json.loads(line)
                    data[j["label"]] = j.get("buckets", [])
                except Exception:
                    pass
    return data


def append_checkpoint(path: Path, batch_map: dict[str, list[str]]):
    with open(path, "a", encoding="utf-8") as f:
        for k, v in batch_map.items():
            f.write(json.dumps({"label": k, "buckets": v}, ensure_ascii=False) + "\n")

def llm_assign_batch(labels: list[str]) -> dict[str, list[str]]:
    bucket_guide = {
        "meal": "Restaurants, cafes, pubs/bars serving full meals (breakfast/lunch/dinner).",
        "accommodation": "Hotels, hostels, homestays, resorts, inns, lodges, guest houses.",
        "attractions/food_culinary": "Desserts, snacks, drinks only (ice cream, tea, boba, juice, chocolatier). NOT full meals.",
        "attractions/adventure": "Thrill activities: zipline, ATV, go-kart, bungee, rafting, diving, paragliding, rock climbing, caves.",
        "attractions/art_museums": "Museums, art galleries, handicraft/batik/pottery workshops, artisan studios.",
        "attractions/family": "Theme parks, water parks, zoos, kid activities.",
        "attractions/cultural_history": "Museums, heritage sites, forts, monuments, palaces, street art, theatres.",
        "attractions/nature": "Parks, gardens, forests, waterfalls, mountains, beaches, islands, lakes, trails.",
        "attractions/nightlife": "Bars, pubs, nightclubs, karaoke, lounges, live music, cocktail bars, rooftops.",
        "attractions/relax": "Spas, wellness centers, massage, sauna, hot springs, baths.",
        "attractions/religious_sites": "Temples, mosques, churches, shrines, pagodas, monasteries.",
        "attractions/shopping": "Malls, markets, bazaars, outlets, boutiques, night markets.",
        "unique": "Niche tourist interests not in main buckets (bookstores, record shops, vintage stores, thrift shops, antique stores). Some tourists seek these for authentic local experiences.",
        "exclude": "Completely non-tourism: offices, embassies, banks, ATMs, schools, universities, hospitals, clinics, dentists, pharmacies, warehouses, factories, logistics, banquet halls, event venues, corporate services, repair shops, auto parts.",
    }

    few_shots = [
        ("banquet hall", ["exclude"]),
        ("event venue", ["exclude"]),
        ("warehouse", ["exclude"]),
        ("barbecue area", ["exclude"]),
        ("pub", ["meal", "attractions/nightlife"]),
        ("adventure sports center", ["attractions/adventure"]),
        ("supermarket", ["exclude"]),
        ("wet market", ["exclude"]),
        ("coffee shop", ["meal"]),
        ("dessert shop", ["attractions/food_culinary"]),
        ("temple", ["attractions/religious_sites"]),
        ("mall", ["attractions/shopping"]),
        ("bookstore", ["unique"]),
        ("record shop", ["unique"]),
        ("vintage clothing store", ["unique"]),
        ("laundromat", ["exclude"]),
        ("antique store", ["unique"]),
        ("wildlife sanctuary", ["unique"]),
        ("tourist attraction", ["unique"]),
        ("3d printing service", ["exclude"]),
        ("lottery retailer", ["exclude"]),
    ]

    guide_lines = "\n".join(f"{k}: {v}" for k, v in bucket_guide.items())
    shot_lines = "\n".join(
        f"{lab} -> {','.join(bkts) if bkts else 'NONE'}" for lab, bkts in few_shots
    )
    labels_str = "\n".join(f"{i + 1}. {lab}" for i, lab in enumerate(labels))

    prompt = f"""Classify each Google Places category label into zero or more tourism buckets.

BUCKETS:
{guide_lines}

EXAMPLES:
{shot_lines}

LABELS TO CLASSIFY:
{labels_str}

Return ONLY valid JSON in this exact format (no markdown, no explanations):
{{"results": [{{"label": "example", "buckets": ["meal"]}}, ...]}}"""

    # 1) Call Gemini; don't use response.text (it throws when finish_reason != STOP).
    try:
        response = model.generate_content(
            prompt,
            generation_config={
                "temperature": 0.0,
                "max_output_tokens": 1024,
            },
        )
    except Exception as e:
        raise RuntimeError(f"Gemini API call failed: {e}") from e

    # 2) Extract raw text safely from candidates
    if not getattr(response, "candidates", None):
        raise RuntimeError("Gemini returned no candidates.")

    candidate = response.candidates[0]

    parts = getattr(candidate, "content", None)
    if not parts or not getattr(parts, "parts", None):
        raise RuntimeError("Gemini candidate has no content parts.")

    texts = []
    for part in parts.parts:
        t = getattr(part, "text", None)
        if t:
            texts.append(t)

    raw_text = "".join(texts).strip()
    if not raw_text:
        raise RuntimeError("Gemini returned empty text content.")

    cleaned = raw_text

    # 3) Strip any markdown fences if the model ignored the instruction
    if "```json" in cleaned:
        cleaned = cleaned.split("```json", 1)[1].split("```", 1)[0].strip()
    elif "```" in cleaned:
        cleaned = cleaned.split("```", 1)[1].split("```", 1)[0].strip()

    # 4) Extract JSON object between first '{' and last '}'
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise RuntimeError(f"Could not find JSON object in response: {cleaned[:200]!r}")

    json_str = cleaned[start : end + 1]

    # 5) Parse JSON
    try:
        parsed = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"JSON parse error: {e}; snippet={json_str[:200]!r}") from e

    results = parsed.get("results")
    if not isinstance(results, list):
        raise RuntimeError(f"Unexpected JSON structure, 'results' missing or not a list: {parsed!r}")

    # 6) Normalise buckets
    allowed = set(ALL_CATEGORY_KEYS + ["unique", "exclude"])
    out: dict[str, list[str]] = {}

    for item in results:
        if not isinstance(item, dict):
            continue
        lab = item.get("label", "")
        if not lab:
            continue
        buckets_raw = item.get("buckets", [])
        if not isinstance(buckets_raw, list):
            buckets_raw = []
        buckets = [b for b in buckets_raw if b in allowed]
        out[lab] = buckets

    # 7) Ensure all labels in batch have an entry (possibly empty)
    for lab in labels:
        out.setdefault(lab, [])

    return out

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "attractions").mkdir(parents=True, exist_ok=True)

    raw = [ln.strip() for ln in open(INPUT_FILE, encoding="utf-8") if ln.strip()]
    assigned = {k: [] for k in ALL_CATEGORY_KEYS}

    ai_exclude = set()
    ai_unique = set()

    # Resume previous AI assignments
    ckpt_path = OUTPUT_DIR / "_ai_assignments.jsonl"
    prev = load_checkpoint(ckpt_path)
    for lab, buckets in prev.items():
        for b in buckets:
            if b in assigned:
                assigned[b].append(lab)
            elif b == "exclude":
                ai_exclude.add(lab)
            elif b == "unique":
                ai_unique.add(lab)

    already = set(prev)
    remaining = [x for x in raw if x not in already]

    if remaining:
        print(f"Classifying {len(remaining)} labels with {MODEL_NAME}...")
        for i in tqdm(
            range(0, len(remaining), BATCH_SIZE), desc="Classifying", ncols=80
        ):
            batch = remaining[i : i + BATCH_SIZE]
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    res = llm_assign_batch(batch)

                    for label, buckets in res.items():
                        if not buckets:
                            continue
                        for b in buckets:
                            if b in assigned:
                                assigned[b].append(label)
                            elif b == "exclude":
                                ai_exclude.add(label)
                            elif b == "unique":
                                ai_unique.add(label)

                    append_checkpoint(ckpt_path, res)
                    break
                except Exception as e:
                    with open(ERROR_LOG, "a", encoding="utf-8") as logf:
                        logf.write(f"Attempt {attempt}/{MAX_RETRIES} failed for batch starting '{batch[0]}': {e}\n")
                    if attempt == MAX_RETRIES:
                        with open(ERROR_LOG, "a", encoding="utf-8") as logf:
                            logf.write(f"[ERROR] Giving up on batch starting '{batch[0]}'\n")
                    # optional: small backoff
                    # import time; time.sleep(0.5)
                    continue

    # Finalize
    matched_any = (
        set().union(*[set(v) for v in assigned.values()])
        if any(assigned.values())
        else set()
    )
    exclude_final = sorted(ai_exclude - matched_any)
    unique = sorted(
        set([x for x in raw if x not in matched_any and x not in exclude_final])
        | ai_unique
    )

    def write_list(p: Path, items):
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(set(items), key=str.lower)))

    write_list(OUTPUT_DIR / "meal.txt", assigned["meal"])
    write_list(OUTPUT_DIR / "accommodation.txt", assigned["accommodation"])
    for sub in ATTRACTIONS:
        write_list(
            OUTPUT_DIR / "attractions" / f"{sub}.txt", assigned[f"attractions/{sub}"]
        )
    write_list(OUTPUT_DIR / "exclude_final.txt", exclude_final)
    write_list(OUTPUT_DIR / "unique.txt", unique)

    # Create indices
    bucket_to_items = {
        "meal": sorted(set(assigned["meal"]), key=str.lower),
        "accommodation": sorted(set(assigned["accommodation"]), key=str.lower),
    }
    for sub in ATTRACTIONS:
        key = f"attractions/{sub}"
        bucket_to_items[key] = sorted(set(assigned[key]), key=str.lower)

    label_to_buckets = {}
    for b, items in bucket_to_items.items():
        for it in items:
            label_to_buckets.setdefault(it, set()).add(b)
    label_to_buckets = {k: sorted(v) for k, v in label_to_buckets.items()}

    with open(OUTPUT_DIR / "bucket_index.json", "w", encoding="utf-8") as f:
        json.dump(bucket_to_items, f, ensure_ascii=False, indent=2)
    with open(OUTPUT_DIR / "label_index.json", "w", encoding="utf-8") as f:
        json.dump(label_to_buckets, f, ensure_ascii=False, indent=2)

    # Planner layer
    PLANNER_DIR = OUTPUT_DIR / "planner"
    PLANNER_DIR.mkdir(parents=True, exist_ok=True)

    PLANNER_POLICY = {
        "defaults": {
            "include_buckets": [
                "meal",
                "accommodation",
                "attractions/food_culinary",
                "attractions/adventure",
                "attractions/art_museums",
                "attractions/family",
                "attractions/cultural_history",
                "attractions/nature",
                "attractions/nightlife",
                "attractions/relax",
                "attractions/religious_sites",
                "attractions/shopping",
            ],
            "exclude_groups": ["unique", "exclude"],
            "block_terms": [
                r"\bwholesale\b",
                r"\bindustrial\b",
                r"\brepair\b",
                r"\bservice\s*center\b",
                r"\bservicing\b",
                r"\bworkshop\b",
            ],
        },
        "themes": {
            "shopping": {
                "include_only_buckets": ["attractions/shopping"],
                "extra_block_terms": [
                    r"\bsupply\b",
                    r"\bsupplies\b",
                    r"\bspare\b",
                    r"\bauto\s*part(s)?\b",
                    r"\bhardware\b",
                    r"\belectronic(s)?\s*(shop|store)\b",
                    r"\bpharmacy\b",
                    r"\bconvenience\s*store\b",
                    r"\bsupermarket\b",
                    r"\bwet\s*market\b",
                    r"\bhypermarket\b",

                    r"\bgeneral\s+store\b",
                    r"\bwedding\b",
                    r"\bdepartment\s+store\b",
                    r"\bstate\s+liquor\b",
                    r"\bbaby\b",
                    r"\bchildren\b",
                    r"\byouth\s+clothing\b",
                    r"\bsport(ing)?\s+goods\b",
                    r"\bsportswear\b",
                    r"\btextile\s+merchant\b",
                ],
            },
            "food_tour": {
                "include_only_buckets": ["meal", "attractions/food_culinary"],
            },
            "culture": {
                "include_only_buckets": [
                    "attractions/cultural_history",
                    "attractions/religious_sites",
                ],
            },
            "nature": {
                "include_only_buckets": ["attractions/nature"],
            },
            "adventure": {
                "include_only_buckets": ["attractions/adventure"],
            },
            "nightlife": {
                "include_only_buckets": ["attractions/nightlife"],
            },
            "relax": {
                "include_only_buckets": ["attractions/relax"],
            },
            "family": {
                "include_only_buckets": ["attractions/family"],
            },
            "stay": {
                "include_only_buckets": ["accommodation"],
            },
        },
    }

    with open(PLANNER_DIR / "policy.yaml", "w", encoding="utf-8") as f:
        yaml.safe_dump(PLANNER_POLICY, f, sort_keys=False, allow_unicode=True)

    def read_list(p):
        if not p.exists():
            return []
        return [ln.strip() for ln in open(p, encoding="utf-8") if ln.strip()]

    exclude_final_items = set(read_list(OUTPUT_DIR / "exclude_final.txt"))
    unique_items = set(read_list(OUTPUT_DIR / "unique.txt"))

    WHITELIST_PATH = PLANNER_DIR / "whitelist.txt"
    BLACKLIST_PATH = PLANNER_DIR / "blacklist.txt"
    for pth in [WHITELIST_PATH, BLACKLIST_PATH]:
        if not pth.exists():
            with open(pth, "w", encoding="utf-8") as f:
                f.write("")

    whitelist = set(read_list(WHITELIST_PATH))
    blacklist = set(read_list(BLACKLIST_PATH))

    def any_matches(text, patterns):
        import re

        t = text.lower().replace("_", " ")
        for pat in patterns:
            if re.search(pat, t, flags=re.I):
                return True
        return False


    def build_theme_list(theme: str, policy: dict) -> list[str]:
        defaults = policy["defaults"]
        cfg = policy["themes"][theme]
        include_only = cfg.get("include_only_buckets")
        also_allow = cfg.get("also_allow_buckets", [])
        extra_blocks = cfg.get("extra_block_terms", [])

        allowed_buckets = set(include_only) if include_only else set(
            defaults["include_buckets"]
        )
        allowed_buckets |= set(also_allow)

        block_terms = list(defaults["block_terms"]) + list(extra_blocks)

        out = []
        for label, buckets in label_to_buckets.items():
            if label in blacklist:
                continue
            if label in exclude_final_items:
                continue
            if label in unique_items and label not in whitelist:
                continue
            if not (set(buckets) & allowed_buckets):
                continue
            if label not in whitelist and any_matches(label, block_terms):
                continue
            out.append(label)

        return sorted(set(out), key=str.lower)

    planner_index = {}
    for theme in PLANNER_POLICY["themes"].keys():
        lst = build_theme_list(theme, PLANNER_POLICY)
        with open(PLANNER_DIR / f"{theme}.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(lst))
        with open(PLANNER_DIR / f"{theme}.json", "w", encoding="utf-8") as f:
            json.dump({"theme": theme, "items": lst}, f, ensure_ascii=False, indent=2)
        planner_index[theme] = lst

    compact = {
        theme: [{"label": it, "slug": slugify(it)} for it in items]
        for theme, items in planner_index.items()
    }
    with open(PLANNER_DIR / "planner_index.json", "w", encoding="utf-8") as f:
        json.dump(compact, f, ensure_ascii=False, indent=2)

    print(f"Done. Output: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
