import shutil
import osmnx as ox
from supabase import create_client
from dotenv import load_dotenv
from shapely.geometry import Polygon, MultiPolygon, mapping

load_dotenv()
sb = create_client(__import__("os").environ["SUPABASE_URL"], __import__("os").environ["SUPABASE_KEY"])

# Configure your regions here
REGIONS = [
    {
        "country": "Singapore",
        "iso2": "SG",
        "states": [],
    },
    {
        "country": "Malaysia",
        "iso2": "MY",
        "states": [
            "Johor", "Kedah", "Kelantan", "Malacca", "Negeri Sembilan",
            "Pahang", "Penang", "Perak", "Perlis", "Sabah",
            "Sarawak", "Selangor", "Terengganu",
            "Kuala Lumpur", "Labuan", "Putrajaya",
        ],
    },
]


def to_multipolygon_geojson(gdf):
    geom = gdf.geometry.iloc[0]
    if isinstance(geom, Polygon):
        geom = MultiPolygon([geom])
    return mapping(geom)


def upsert_area(name: str, country_iso2: str, kind: str, admin_level: int, geom_geojson: dict):
    res = sb.rpc("rpc_upsert_admin_area_geojson", {
        "p_name": name,
        "p_country_iso2": country_iso2,
        "p_kind": kind,
        "p_admin_level": admin_level,
        "p_parent_id": None,
        "p_geom_geojson": geom_geojson,
    }).execute()
    return res.data


def fetch_and_upsert_country(name: str, iso2: str):
    print(f"Fetching {name}...")
    gdf = ox.geocode_to_gdf(name)
    geom = to_multipolygon_geojson(gdf)
    uid = upsert_area(name, iso2, "country", 2, geom)
    print(f"✅ {name} id: {uid}")
    return uid


def fetch_and_upsert_states(country: str, iso2: str, states: list[str]):
    ok = fail = 0
    for state in states:
        try:
            query = f"{state}, {country}"
            print(f"  Fetching {state}...")
            gdf = ox.geocode_to_gdf(query)
            geom = to_multipolygon_geojson(gdf)
            upsert_area(state, iso2, "state", 4, geom)
            ok += 1
        except Exception as e:
            print(f"  ✗ FAILED: {state} - {e}")
            fail += 1
    if states:
        print(f"✅ {country} states: {ok}/{len(states)}")


if __name__ == "__main__":
    for region in REGIONS:
        fetch_and_upsert_country(region["country"], region["iso2"])
        if region["states"]:
            fetch_and_upsert_states(region["country"], region["iso2"], region["states"])

    shutil.rmtree("cache", ignore_errors=True)
