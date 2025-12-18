-- Table updates for multi-language support
ALTER TABLE pois DROP COLUMN IF EXISTS name_i18n;
DROP INDEX IF EXISTS pois_name_i18n_trgm;

-- POI search function
DROP FUNCTION IF EXISTS rpc_search_pois;

CREATE OR REPLACE FUNCTION rpc_search_pois(
  p_mode text DEFAULT 'list',
  p_destination text DEFAULT NULL,
  p_roles text[] DEFAULT NULL,
  p_query text DEFAULT NULL,
  p_limit integer DEFAULT 5,
  p_offset integer DEFAULT 0
) RETURNS TABLE (
  id uuid,
  name text,
  google_map_link text,
  categories text[],
  address text,
  website text,
  phone text,
  roles poi_role[],
  open_hours jsonb,
  review_count integer,
  review_rating numeric,
  complete_address jsonb,
  descriptions text,
  price_level numeric,
  images text[],
  latitude double precision,
  longitude double precision,
  themes theme_key[],

  kids_friendly boolean,
  pets_friendly boolean,
  wheelchair_accessible_entrance boolean,
  wheelchair_accessible_seating boolean,
  wheelchair_accessible_toilet boolean,
  wheelchair_accessible_car_park boolean,
  halal_food boolean,
  vegan_options boolean,
  vegetarian_options boolean,

  total_count bigint
) LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_dest_id uuid;
  v_dest_kind text;
  v_area_name text;
  v_country_part text;
  v_search_areas uuid[];
  v_total_count bigint;
BEGIN
  -- Parse destination (same as before)
  IF p_destination IS NOT NULL THEN
    v_area_name := trim(split_part(p_destination, ',', 1));
    
    IF p_destination LIKE '%,%' THEN
      v_country_part := NULLIF(trim(split_part(p_destination, ',', 2)), '');
    END IF;
    
    SELECT a.id, a.kind INTO v_dest_id, v_dest_kind
    FROM admin_areas a
    WHERE (
      lower(a.name) = lower(v_area_name)
      OR (v_country_part IS NOT NULL AND a.kind = 'country' AND 
          (lower(a.name) = lower(v_country_part) OR a.country_iso2 = upper(v_country_part)))
    )
    ORDER BY 
      CASE WHEN lower(a.name) = lower(v_area_name) THEN 1 ELSE 2 END,
      CASE WHEN a.kind = 'state' THEN 2 WHEN a.kind = 'country' THEN 3 ELSE 4 END
    LIMIT 1;
    
    IF v_dest_id IS NULL THEN
      RETURN;
    END IF;
    
    IF v_dest_kind = 'country' THEN
      SELECT array_agg(aa.id) INTO v_search_areas
      FROM admin_areas aa
      WHERE aa.id = v_dest_id OR aa.parent_id = v_dest_id;
    ELSE
      v_search_areas := ARRAY[v_dest_id];
    END IF;
  END IF;
  
  -- MODE: list or search
  IF p_mode IN ('list', 'search') THEN
    -- First, get total count
    SELECT COUNT(*) INTO v_total_count
    FROM pois p
    WHERE 
      (p_destination IS NULL OR p.admin_area_id = ANY(v_search_areas))
      AND (p_roles IS NULL OR p.poi_roles::text[] && p_roles)
      AND (
        p_mode = 'list'
        OR p_query IS NULL
        OR lower(p.name) ILIKE '%' || lower(p_query) || '%'
        OR lower(p.descriptions) ILIKE '%' || lower(p_query) || '%'
        OR lower(p.address) ILIKE '%' || lower(p_query) || '%'
      );
    
    -- Then return paginated results with count
    RETURN QUERY
    SELECT 
      fp.id,
      fp.name,
      fp.google_map_link,
      fp.categories,
      fp.address,
      fp.website,
      fp.phone,
      fp.poi_roles,
      fp.open_hours,
      fp.review_count,
      fp.review_rating,
      fp.complete_address,
      fp.descriptions,
      fp.price_level,
      fp.images,
      NULL::double precision,
      NULL::double precision,
      NULL::theme_key[],

      fp.kids_friendly boolean,
      fp.pets_friendly boolean,
      fp.wheelchair_accessible_entrance boolean,
      fp.wheelchair_accessible_seating boolean,
      fp.wheelchair_accessible_toilet boolean,
      fp.wheelchair_accessible_car_park boolean,
      fp.halal_food boolean,
      fp.vegan_options boolean,
      fp.vegetarian_options boolean,

      v_total_count
    FROM (
      SELECT p.*
      FROM pois p
      WHERE 
        (p_destination IS NULL OR p.admin_area_id = ANY(v_search_areas))
        AND (p_roles IS NULL OR p.poi_roles::text[] && p_roles)
        AND (
          p_mode = 'list'
          OR p_query IS NULL
          OR lower(p.name) ILIKE '%' || lower(p_query) || '%'
          OR lower(p.descriptions) ILIKE '%' || lower(p_query) || '%'
          OR lower(p.address) ILIKE '%' || lower(p_query) || '%'
        )
      ORDER BY p.review_count DESC NULLS LAST, p.review_rating DESC NULLS LAST
      LIMIT p_limit OFFSET p_offset
    ) fp;
    
  -- MODE: search_minimal (no pagination, no count needed)
  ELSIF p_mode = 'search_minimal' THEN
    RETURN QUERY
    WITH filtered_pois AS (
      SELECT p.*
      FROM pois p
      WHERE 
        (p_destination IS NULL OR p.admin_area_id = ANY(v_search_areas))
        AND (p_roles IS NULL OR p.poi_roles::text[] && p_roles)
        AND (
          p_query IS NULL
          OR lower(p.name) % lower(p_query)
          OR lower(p.name) LIKE lower(p_query) || '%'
          OR (p.categories IS NOT NULL AND array_length(p.categories, 1) > 0 AND 
              lower(p.categories[1]) % lower(p_query))
        )
    ),
    with_themes AS (
      SELECT 
        fp.*,
        COALESCE((
          SELECT array_agg(DISTINCT m.theme)::theme_key[]
          FROM unnest(fp.categories) AS c
          JOIN theme_category_map m ON m.category = c
        ), ARRAY[]::theme_key[]) AS themes,
        CASE WHEN p_query IS NULL THEN 0
             ELSE similarity(lower(fp.name), lower(p_query)) END AS sim_name,
        CASE WHEN p_query IS NULL THEN 0
             WHEN lower(fp.name) LIKE lower(p_query) || '%' THEN 1
             ELSE 0 END AS has_prefix
      FROM filtered_pois fp
    )
    SELECT 
      wt.id,
      wt.name,
      NULL::text,
      NULL::text[],
      NULL::text,
      NULL::text,
      NULL::text,
      wt.poi_roles,
      wt.open_hours,
      NULL::integer,
      NULL::numeric,
      NULL::jsonb,
      NULL::text,
      NULL::numeric,
      CASE WHEN wt.images IS NULL OR array_length(wt.images, 1) = 0 
           THEN ARRAY[]::text[] ELSE ARRAY[wt.images[1]] END,
      wt.latitude,
      wt.longitude,
      wt.themes,

      wt.kids_friendly,
      wt.pets_friendly,
      wt.wheelchair_accessible_entrance,
      wt.wheelchair_accessible_seating,
      wt.wheelchair_accessible_toilet,
      wt.wheelchair_accessible_car_park,
      wt.halal_food,
      wt.vegan_options,
      wt.vegetarian_options,

      NULL::bigint  -- no total count for minimal
    FROM with_themes wt
    WHERE p_query IS NULL OR wt.sim_name > 0
    ORDER BY wt.has_prefix DESC, wt.sim_name DESC, 
             wt.review_rating DESC NULLS LAST, wt.review_count DESC NULLS LAST
    LIMIT p_limit;
  END IF;
END;
$$;