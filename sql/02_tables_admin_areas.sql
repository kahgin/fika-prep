DROP TABLE IF EXISTS admin_areas CASCADE;

CREATE TABLE admin_areas (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  country_iso2 char(2) NOT NULL,
  kind text NOT NULL,
  admin_level int NOT NULL,
  parent_id uuid REFERENCES admin_areas(id),
  geom geometry(MULTIPOLYGON,4326) NOT NULL,
  UNIQUE (name, country_iso2, kind)
);

DROP INDEX IF EXISTS admin_areas_geom_gist;
DROP INDEX IF EXISTS admin_areas_parent_idx;
DROP INDEX IF EXISTS admin_areas_country_idx;
DROP INDEX IF EXISTS admin_areas_kind_idx;

CREATE INDEX IF NOT EXISTS admin_areas_geom_gist ON admin_areas USING GIST (geom);
CREATE INDEX IF NOT EXISTS admin_areas_parent_idx ON admin_areas(parent_id);
CREATE INDEX IF NOT EXISTS admin_areas_country_idx ON admin_areas(country_iso2);
CREATE INDEX IF NOT EXISTS admin_areas_kind_idx ON admin_areas(kind);

DROP FUNCTION IF EXISTS rpc_upsert_admin_area_geojson;

CREATE OR REPLACE FUNCTION rpc_upsert_admin_area_geojson(
  p_name text,
  p_country_iso2 char(2),
  p_kind text,
  p_admin_level int,
  p_geom_geojson jsonb,
  p_parent_id uuid DEFAULT NULL
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_geom geometry;
  v_id uuid;
BEGIN
  v_geom := ST_SetSRID(ST_GeomFromGeoJSON(p_geom_geojson::text), 4326);
  v_geom := ST_Force2D(v_geom);
  IF GeometryType(v_geom) = 'POLYGON' THEN
    v_geom := ST_Multi(v_geom);
  ELSIF GeometryType(v_geom) NOT IN ('MULTIPOLYGON', 'GEOMETRYCOLLECTION') THEN
    RAISE EXCEPTION 'Geometry must be Polygon or MultiPolygon, got %', GeometryType(v_geom);
  END IF;

  INSERT INTO admin_areas (name, country_iso2, kind, admin_level, parent_id, geom)
  VALUES (p_name, p_country_iso2, p_kind, p_admin_level, p_parent_id, v_geom)
  ON CONFLICT (name, country_iso2, kind) DO UPDATE
  SET admin_level = EXCLUDED.admin_level,
      parent_id = EXCLUDED.parent_id,
      geom = EXCLUDED.geom
  RETURNING id INTO v_id;

  RETURN v_id;
END;
$$;

ALTER TABLE admin_areas ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read access" ON admin_areas FOR SELECT USING (true);
