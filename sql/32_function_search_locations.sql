CREATE OR REPLACE FUNCTION get_country_name(iso2 char(2))
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = public
AS $$
  SELECT CASE
    WHEN iso2 = 'SG' THEN 'Singapore'
    WHEN iso2 = 'MY' THEN 'Malaysia'
    ELSE iso2
  END;
$$;

-- Create trigram index for admin areas
CREATE INDEX IF NOT EXISTS admin_areas_name_trgm ON admin_areas USING GIN (lower(name) gin_trgm_ops);

-- Drop old function
DROP FUNCTION IF EXISTS rpc_search_locations;

-- Updated admin areas search function
CREATE OR REPLACE FUNCTION rpc_search_locations(
  p_query text,
  p_limit int DEFAULT 5
)
RETURNS TABLE (
  id uuid,
  name text,
  label text,
  kind text,
  country_iso2 char(2),
  parent_id uuid,
  admin_level int
)
LANGUAGE sql
STABLE
SET search_path = public
AS $$
  SELECT DISTINCT ON (a.id)
    a.id,
    a.name,
    CASE 
      WHEN a.kind = 'country' THEN a.name
      WHEN a.kind = 'state' THEN a.name || ', ' || get_country_name(a.country_iso2)
      ELSE a.name || ', ' || get_country_name(a.country_iso2)
    END AS label,
    a.kind,
    a.country_iso2,
    a.parent_id,
    a.admin_level
  FROM admin_areas a
  WHERE
    lower(a.name) % lower(p_query)
    OR lower(a.name) LIKE lower(p_query) || '%'
  ORDER BY
    a.id,
    CASE WHEN lower(a.name) LIKE lower(p_query) || '%'
         THEN 0 ELSE 1 END,
    similarity(lower(a.name), lower(p_query)) DESC,
    length(a.name)
  LIMIT p_limit;
$$;
