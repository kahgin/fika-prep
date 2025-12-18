import os
import json
import re
from supabase import create_client
from dotenv import load_dotenv
from shapely.geometry import shape, Polygon, MultiPolygon
from shapely.wkb import dumps as wkb_dumps, loads as wkb_loads

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

SINGAPORE_COUNTRY_GEOJSON = "data/geojson/singapore.geojson"
MALAYSIA_COUNTRY_GEOJSON = "data/geojson/malaysia.geojson"
MALAYSIA_STATE_GEOJSON = "data/geojson/malaysia_states.geojson"

def to_2d_multipolygon(geom_dict):
    g = shape(geom_dict)
    g2d = wkb_loads(wkb_dumps(g, output_dimension=2))
    if isinstance(g2d, Polygon):
        g2d = MultiPolygon([g2d])
    return json.loads(json.dumps(g2d.__geo_interface__))

def drop_z_coords(geom_dict):
    t = geom_dict["type"]
    coords = geom_dict["coordinates"]
    def xy(pt):
        return pt[:2]
    if t == "Polygon":
        return {"type": "Polygon", "coordinates": [[xy(p) for p in ring] for ring in coords]}
    if t == "MultiPolygon":
        return {"type": "MultiPolygon", "coordinates": [[[xy(p) for p in ring] for ring in poly] for poly in coords]}
    raise ValueError(f"Unsupported geometry type: {t}")

def extract_planning_area_name(props):
    desc = props.get("Description") or props.get("description") or ""
    m = re.search(r"PLN_AREA_N<\/th>\s*<td>([^<]+)<\/td>", desc)
    if m:
        return m.group(1).strip().title()
    raise ValueError("Cannot extract planning area name")

def upsert_singapore_country():
    """Insert Singapore as country"""
    with open(SINGAPORE_COUNTRY_GEOJSON, "r", encoding="utf-8") as f:
        fc = json.load(f)
    geom_raw = fc["features"][0]["geometry"]
    geom_2dmp = to_2d_multipolygon(geom_raw)
    res = sb.rpc("rpc_upsert_admin_area_geojson", {
        "p_name": "Singapore",
        "p_country_iso2": "SG",
        "p_kind": "country",
        "p_admin_level": 2,
        "p_parent_id": None,
        "p_geom_geojson": geom_2dmp
    }).execute()
    print("✅ Singapore country id:", res.data)
    return res.data

def upsert_malaysia_country():
    """Insert Malaysia as country"""
    with open(MALAYSIA_COUNTRY_GEOJSON, "r", encoding="utf-8") as f:
        fc = json.load(f)
    
    # Find the Malaysia country feature (not states)
    malaysia_feature = None
    for feat in fc["features"]:
        if feat["properties"].get("name") == "Malaysia":
            malaysia_feature = feat
            break
    
    if not malaysia_feature:
        raise ValueError("Malaysia country feature not found in GeoJSON")
    
    geom_raw = malaysia_feature["geometry"]
    geom_2dmp = to_2d_multipolygon(geom_raw)
    res = sb.rpc("rpc_upsert_admin_area_geojson", {
        "p_name": "Malaysia",
        "p_country_iso2": "MY",
        "p_kind": "country",
        "p_admin_level": 2,
        "p_parent_id": None,
        "p_geom_geojson": geom_2dmp
    }).execute()
    print("✅ Malaysia country id:", res.data)
    return res.data

def upsert_malaysia_states():
    """Insert Malaysian states"""
    with open(MALAYSIA_STATE_GEOJSON, "r", encoding="utf-8") as f:
        fc = json.load(f)
    
    # Filter only state features (those with state IDs like MY01, MY12, etc.)
    state_features = [
        feat for feat in fc["features"] 
        if feat["properties"].get("id", "").startswith("MY") 
        and len(feat["properties"].get("id", "")) > 2  # MY12, MY01, etc.
    ]
    
    ok = fail = 0
    for feat in state_features:
        try:
            props = feat.get("properties", {})
            name = props.get("name")
            state_id = props.get("id")
            
            if not name:
                print(f"⚠️  Skipping feature with no name: {state_id}")
                continue
            
            geom_raw = feat["geometry"]
            geom_2dmp = to_2d_multipolygon(geom_raw)
            
            sb.rpc("rpc_upsert_admin_area_geojson", {
                "p_name": name,
                "p_country_iso2": "MY",
                "p_kind": "state",
                "p_admin_level": 4,
                "p_geom_geojson": geom_2dmp,
                "p_parent_id": None,  # Will be set later via ST_Intersects
            }).execute()
            
            ok += 1
            
        except Exception as e:
            fail += 1
            name = props.get("name", "?") if 'props' in locals() else "?"
            print(f"  ✗ FAILED: {name} - {e}")
    
    print(f"✅ Malaysia states done. ok={ok} fail={fail}")

if __name__ == "__main__":
    
    # 1. Insert countries
    upsert_singapore_country()
    upsert_malaysia_country()
    
    # 2. Insert Malaysian states
    upsert_malaysia_states()
