DROP FUNCTION IF EXISTS rpc_fetch_poi_candidates_quota;

CREATE OR REPLACE FUNCTION rpc_fetch_poi_candidates_quota(
  p_destination text,
  p_themes text[],
  p_quota_attraction int,
  p_quota_meal int,
  p_quota_accommodation int,

  p_roles text[] DEFAULT ARRAY['attraction','meal','accommodation'],
  p_min_rating numeric DEFAULT 2.0,
  p_min_reviews int DEFAULT 10,
  p_per_area_cap int DEFAULT 150,
  p_halal_only boolean DEFAULT false,
  p_vegetarian_only boolean DEFAULT false,
  p_vegan_only boolean DEFAULT false,
  p_wheelchair_only boolean DEFAULT false,
  p_kids_friendly_only boolean DEFAULT false,
  p_pets_friendly_only boolean DEFAULT false,
  p_include_images boolean DEFAULT true,
  p_excluded_themes text[] DEFAULT NULL,
  p_seed_lon numeric DEFAULT NULL,
  p_seed_lat numeric DEFAULT NULL
)
RETURNS TABLE (
  id uuid,
  name text,
  categories text[],
  themes theme_key[],
  poi_roles poi_role[],
  open_hours jsonb,
  review_count int,
  review_rating numeric,
  latitude double precision,
  longitude double precision,
--   price_level numeric,
  images text[],

  kids_friendly boolean,
  pets_friendly boolean,
  wheelchair_accessible_entrance boolean,
  wheelchair_accessible_seating boolean,
  wheelchair_accessible_toilet boolean,
  halal_food boolean,
  vegan_options boolean,
  vegetarian_options boolean,

  role_pick text,
  area_name text,
  distance_m double precision
)
LANGUAGE sql STABLE AS
$$
WITH dest AS (
  SELECT id, kind, geom
  FROM admin_areas
  WHERE name ILIKE SPLIT_PART(p_destination, ',', 1)
  ORDER BY CASE kind WHEN 'country' THEN 2 WHEN 'city' THEN 1 ELSE 0 END DESC
  LIMIT 1
),
cover AS (
  SELECT id, geom FROM dest
  UNION
  SELECT aa.id, aa.geom
  FROM admin_areas aa
  JOIN dest d ON aa.parent_id = d.id
),
seed AS (
  SELECT CASE
           WHEN p_seed_lon IS NOT NULL AND p_seed_lat IS NOT NULL
           THEN ST_SetSRID(ST_MakePoint(p_seed_lon, p_seed_lat), 4326)
           ELSE NULL::geometry
         END AS g
),
base AS (
  SELECT DISTINCT p.*, a.id AS area_id, a.geom AS area_geom,
  CASE
    WHEN 'attraction' = ANY(p.poi_roles)
      THEN (
        SELECT COALESCE(array_agg(DISTINCT t.theme)::theme_key[], ARRAY[]::theme_key[])
        FROM unnest(p.categories) AS c
        JOIN theme_category_map t ON t.category = c
      )
    ELSE ARRAY[]::theme_key[]
  END AS themes
  FROM pois p
  JOIN LATERAL (
     SELECT id, geom
     FROM cover
     WHERE p.geom IS NOT NULL
       AND ST_Intersects(p.geom::geometry, geom)
     ORDER BY ST_Area(geom) ASC NULLS LAST
     LIMIT 1
  ) a ON TRUE
  WHERE
    p.poi_roles::text[] && p_roles::text[]
    AND p.review_rating >= p_min_rating
    AND p.review_count >= p_min_reviews
    AND (
        'accommodation' = ANY(p.poi_roles)
        OR EXISTS (
            SELECT 1
            FROM unnest(p.categories) AS c
            JOIN theme_category_map t ON t.category = c
            WHERE t.theme = ANY (p_themes::theme_key[])
        )
    )
    AND (
      NOT ('meal' = ANY(p.poi_roles)) OR (
        (NOT p_halal_only OR COALESCE(p.halal_food, false))
        AND (NOT p_vegetarian_only OR COALESCE(p.vegetarian_options, false) OR COALESCE(p.vegan_options, false))
        AND (NOT p_vegan_only OR COALESCE(p.vegan_options, false))
      )
    )
    AND (
      NOT p_wheelchair_only
      OR COALESCE(p.wheelchair_accessible_entrance, false)
      OR COALESCE(p.wheelchair_accessible_seating, false)
      OR COALESCE(p.wheelchair_accessible_toilet, false)
    )
    AND (
      p_excluded_themes IS NULL
      OR array_length(p_excluded_themes,1) IS NULL
      OR NOT ('attraction' = ANY(p.poi_roles))
      OR NOT EXISTS (
          SELECT 1
          FROM unnest(p.categories) AS c
          JOIN theme_category_map t ON t.category = c
          WHERE t.theme::text = ANY (p_excluded_themes)
      )
    )
    AND (
      NOT p_kids_friendly_only OR CASE WHEN ('attraction' = ANY(p.poi_roles) OR 'meal' = ANY(p.poi_roles)) THEN COALESCE(p.kids_friendly, false) ELSE true END
    )
    AND (
      NOT p_pets_friendly_only OR CASE WHEN ('attraction' = ANY(p.poi_roles) OR 'meal' = ANY(p.poi_roles)) THEN COALESCE(p.pets_friendly, false) ELSE true END
    )
),
scored AS (
  SELECT
    b.*,
    aa.name AS area_name,
    CASE WHEN (SELECT g FROM seed) IS NOT NULL
         THEN ST_Distance(b.geom::geography, (SELECT g FROM seed)::geography)
         ELSE NULL::double precision
    END AS distance_m
  FROM base b
  LEFT JOIN admin_areas aa ON aa.id = b.area_id
),
ranked AS (
  SELECT
    s.*,
    r.role AS role_pick,
    ROW_NUMBER() OVER (
      PARTITION BY r.role, s.area_id
      ORDER BY
        CASE WHEN s.distance_m IS NULL THEN 0 ELSE s.distance_m END ASC,
        s.review_rating DESC,
        s.review_count DESC
    ) AS rn_area
  FROM scored s
  JOIN unnest(p_roles) AS r(role) ON r.role::text = ANY (s.poi_roles::text[])
),
capped_area AS (
  SELECT * FROM ranked WHERE rn_area <= p_per_area_cap
),
role_quota AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY role_pick
           ORDER BY
             CASE WHEN distance_m IS NULL THEN 0 ELSE distance_m END ASC,
             review_rating DESC,
             review_count DESC
         ) AS rn_role
  FROM capped_area
),
final_rank AS (
  SELECT rq.*,
         ROW_NUMBER() OVER (
           PARTITION BY id
           ORDER BY
             CASE role_pick WHEN 'attraction' THEN 1 WHEN 'meal' THEN 2 ELSE 3 END,
             review_rating DESC,
             review_count DESC
         ) AS rn_id
  FROM role_quota rq
)
SELECT
  id,
  name,
  categories,
  themes,
  poi_roles,
  open_hours,
  review_count,
  review_rating,
  latitude,
  longitude,
--   price_level,
  CASE WHEN p_include_images THEN COALESCE(ARRAY[images[1]], ARRAY[]::text[]) ELSE ARRAY[]::text[] END AS images,
  kids_friendly,
  pets_friendly,
  wheelchair_accessible_entrance,
  wheelchair_accessible_seating,
  wheelchair_accessible_toilet,
  halal_food,
  vegan_options,
  vegetarian_options,

  role_pick,
  area_name,
  distance_m
FROM final_rank
WHERE rn_id = 1
  AND (
    (role_pick = 'attraction' AND rn_role <= p_quota_attraction)
    OR (role_pick = 'meal' AND rn_role <= p_quota_meal)
    OR (role_pick = 'accommodation' AND rn_role <= p_quota_accommodation)
  );
$$;
