DROP TABLE IF EXISTS theme_category_map;
DROP TABLE IF EXISTS category_role_map;
DROP TABLE IF EXISTS pois;

CREATE TABLE pois (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz DEFAULT now(),

  google_map_link text UNIQUE NOT NULL,
  name text NOT NULL,
  categories text[],
  address text,
  website text,
  phone text,
  poi_roles poi_role[],

  timezone text,
  open_hours jsonb,

  review_count integer,
  review_rating numeric(2,1),
  -- reviews_per_rating jsonb,

  latitude double precision,
  longitude double precision,
  geom GEOGRAPHY(POINT, 4326),
  complete_address jsonb,

  descriptions text,
  price_level numeric(2,1),

  images text[],
  -- videos text[],

  -- about jsonb,

  kids_friendly boolean DEFAULT false,
  pets_friendly boolean DEFAULT false,
  wheelchair_rental boolean DEFAULT false,
  wheelchair_accessible_car_park boolean DEFAULT false,
  wheelchair_accessible_entrance boolean DEFAULT false,
  wheelchair_accessible_seating boolean DEFAULT false,
  wheelchair_accessible_toilet boolean DEFAULT false,
  halal_food boolean DEFAULT false,
  vegan_options boolean DEFAULT false,
  vegetarian_options boolean DEFAULT false,
  reservations_required boolean DEFAULT false
);

CREATE OR REPLACE FUNCTION pois_set_geom()
RETURNS trigger AS $$
BEGIN
  IF NEW.longitude IS NOT NULL AND NEW.latitude IS NOT NULL THEN
    NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326)::geography;
  ELSE
    NEW.geom := NULL;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_pois_set_geom ON pois;
CREATE TRIGGER trg_pois_set_geom
BEFORE INSERT OR UPDATE ON pois
FOR EACH ROW
EXECUTE FUNCTION pois_set_geom();

CREATE INDEX idx_pois_categories ON pois USING GIN (categories);
CREATE INDEX idx_pois_roles_gin ON pois USING GIN (poi_roles);
CREATE INDEX idx_pois_geom ON pois USING GIST (geom);
CREATE INDEX idx_pois_rating ON pois (review_rating) WHERE review_rating IS NOT NULL;
CREATE INDEX idx_pois_kids ON pois (kids_friendly) WHERE kids_friendly = true;
CREATE INDEX idx_pois_pets ON pois (pets_friendly) WHERE pets_friendly = true;
CREATE INDEX idx_pois_wheelchair ON pois (wheelchair_accessible_entrance) WHERE wheelchair_accessible_entrance = true;
CREATE INDEX idx_pois_halal ON pois (halal_food) WHERE halal_food = true;
CREATE INDEX idx_pois_vegan_true ON pois (vegan_options) WHERE vegan_options = true;
CREATE INDEX idx_pois_vegetarian_true ON pois (vegetarian_options) WHERE vegetarian_options = true;
CREATE INDEX idx_pois_reservation_true ON pois (reservations_required) WHERE reservations_required = true;

ALTER TABLE pois ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read access" ON pois FOR SELECT USING (true);

CREATE TABLE category_role_map (
  category text NOT NULL,
  role poi_role NOT NULL,
  PRIMARY KEY (category, role)
);
CREATE INDEX idx_crm_category ON category_role_map (category);
CREATE INDEX idx_crm_role ON category_role_map (role);

CREATE OR REPLACE FUNCTION pois_set_roles()
RETURNS trigger AS $$
DECLARE
  computed_roles poi_role[];
  has_accommodation boolean;
  has_meal boolean;
BEGIN
  IF NEW.categories IS NULL OR array_length(NEW.categories,1) IS NULL THEN
    NEW.poi_roles := ARRAY[]::poi_role[];
  ELSE
    SELECT COALESCE(
      ARRAY(
        SELECT DISTINCT m.role
        FROM unnest(NEW.categories) AS c
        JOIN category_role_map m ON m.category = c
      ),
      ARRAY[]::poi_role[]
    )
    INTO computed_roles;
    
    -- Check if both accommodation and meal roles exist
    has_accommodation := 'accommodation'::poi_role = ANY(computed_roles);
    has_meal := 'meal'::poi_role = ANY(computed_roles);
    
    -- If both exist, remove meal role
    IF has_accommodation AND has_meal THEN
      computed_roles := ARRAY(
        SELECT role 
        FROM unnest(computed_roles) AS role 
        WHERE role != 'meal'::poi_role
      );
    END IF;
    
    NEW.poi_roles := computed_roles;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_pois_set_roles ON pois;
CREATE TRIGGER trg_pois_set_roles
BEFORE INSERT OR UPDATE OF categories ON pois
FOR EACH ROW
EXECUTE FUNCTION pois_set_roles();

CREATE TABLE theme_category_map (
  theme theme_key NOT NULL,
  category text NOT NULL,
  PRIMARY KEY (theme, category)
);
CREATE INDEX idx_tcm_theme ON theme_category_map (theme);
CREATE INDEX idx_tcm_category ON theme_category_map (category);
