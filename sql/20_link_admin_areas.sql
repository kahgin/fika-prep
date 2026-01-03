UPDATE admin_areas child
SET parent_id = parent.id
FROM admin_areas parent
WHERE child.kind = 'state'
  AND parent.kind = 'country'
  AND parent.country_iso2 = child.country_iso2
  AND ST_Intersects(child.geom, parent.geom)
  AND child.parent_id IS NULL;


ALTER TABLE pois ADD COLUMN IF NOT EXISTS admin_area_id uuid;
CREATE INDEX IF NOT EXISTS idx_pois_admin_area ON pois(admin_area_id);

-- Assign POIs to the most specific admin area they intersect with
WITH hits AS (
  SELECT p.id AS poi_id, a.id AS area_id,
         CASE a.kind 
           WHEN 'district' THEN 3   -- Most specific (future use)
           WHEN 'state' THEN 2       -- Specific
           WHEN 'country' THEN 1     -- Fallback
           ELSE 0 
         END AS rank
  FROM pois p
  JOIN admin_areas a
    ON p.geom IS NOT NULL
   AND ST_Intersects(p.geom::geometry, a.geom)
   AND a.kind IN ('country', 'state')  -- Add 'district' for future use
),
pick AS (
  SELECT poi_id, area_id
  FROM (
    SELECT poi_id, area_id,
           ROW_NUMBER() OVER (PARTITION BY poi_id ORDER BY rank DESC) rn
    FROM hits
  ) z
  WHERE rn = 1
)
UPDATE pois p
SET admin_area_id = pick.area_id
FROM pick
WHERE p.id = pick.poi_id;
