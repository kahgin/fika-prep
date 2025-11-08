import os, json, re
from supabase import create_client
from dotenv import load_dotenv
from shapely.geometry import shape, Polygon, MultiPolygon
from shapely.wkb import dumps as wkb_dumps, loads as wkb_loads

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

SINGAPORE_COUNTRY_GEOJSON = "geojson/singapore.geojson"
PLANNING_AREAS_GEOJSON   = "geojson/MasterPlan2019PlanningAreaBoundaryNoSea.geojson"

def to_2d_multipolygon(geom_dict):
    g = shape(geom_dict)
    g2d = wkb_loads(wkb_dumps(g, output_dimension=2))
    if isinstance(g2d, Polygon):
        g2d = MultiPolygon([g2d])
    return json.loads(json.dumps(g2d.__geo_interface__))

def drop_z_coords(geom_dict):
    t = geom_dict["type"]
    coords = geom_dict["coordinates"]
    def xy(pt): return pt[:2]
    if t == "Polygon":
        return {"type":"Polygon", "coordinates":[[xy(p) for p in ring] for ring in coords]}
    if t == "MultiPolygon":
        return {"type":"MultiPolygon", "coordinates":[[[xy(p) for p in ring] for ring in poly] for poly in coords]}
    raise ValueError(f"Unsupported geometry type: {t}")

def extract_planning_area_name(props):
    for k in ("PLN_AREA_N","pln_area_n","Name","name"):
        if k in props and props[k]:
            return str(props[k]).strip().title()
    desc = props.get("Description") or props.get("description") or ""
    m = re.search(r"PLN_AREA_N<\/th>\s*<td>([^<]+)<", desc, flags=re.I)
    if m:
        return m.group(1).strip().title()
    raise ValueError("Cannot extract planning area name")

# load Singapore country polygon from OSM
def upsert_country():
    with open(SINGAPORE_COUNTRY_GEOJSON, "r", encoding="utf-8") as f:
        fc = json.load(f)
    geom_raw = fc["features"][0]["geometry"]
    geom_2dmp = to_2d_multipolygon(geom_raw)
    res = sb.rpc("rpc_upsert_admin_area_geojson", {
        "p_name": "Singapore",
        "p_country_iso2": "SG",
        "p_kind": "country",
        "p_admin_level": 2,
        "p_geom_geojson": geom_2dmp
    }).execute()
    print("Country SG id:", res.data)

# load Singapore planning area polygons from data.gov.sg
def upsert_planning_areas():
    with open(PLANNING_AREAS_GEOJSON, "r", encoding="utf-8") as f:
        fc = json.load(f)
    feats = fc["features"]
    print("planning area features:", len(feats))
    ok = fail = 0
    for ft in feats:
        try:
            name = extract_planning_area_name(ft.get("properties", {}))
            geom_2d = drop_z_coords(ft["geometry"])
            geom_2dmp = to_2d_multipolygon(geom_2d)
            sb.rpc("rpc_upsert_admin_area_geojson", {
                "p_name": name,
                "p_country_iso2": "SG",
                "p_kind": "planning_area",
                "p_admin_level": None,
                "p_geom_geojson": geom_2dmp
            }).execute()
            ok += 1
        except Exception as e:
            fail += 1
            print("FAILED:", name if 'name' in locals() else '?', e)
    print(f"done planning areas. ok={ok} fail={fail}")

if __name__ == "__main__":
    upsert_country()
    upsert_planning_areas()
    print("All upserts sent.")
