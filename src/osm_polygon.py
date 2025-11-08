import osmnx as ox
import geopandas as gpd
import json

place = "Singapore"

# 1. Get polygon
gdf = ox.geocode_to_gdf(place)

geom = gdf.geometry.iloc[0]

# 2. Ensure it's a valid MultiPolygon
if geom.geom_type == "Polygon":
    geom = geom.buffer(0)
    geom = gpd.GeoSeries([geom]).set_crs(4326).union_all
    geom = gpd.GeoSeries([geom]).set_crs(4326)[0]
    geom = ox.utils_geo._multi_polygon_from_polygon(geom)

# 3. Convert to proper GeoJSON
gdf2 = gpd.GeoDataFrame({"name": [place]}, geometry=[geom], crs=4326)
geojson = json.loads(gdf2.to_json())

# 4. Save
with open("geojson/singapore.geojson", "w") as f:
    json.dump(geojson, f)

print("Saved geojson/singapore.geojson")
