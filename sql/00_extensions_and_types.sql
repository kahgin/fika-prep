-- Extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- ENUMs
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'poi_role') THEN
    CREATE TYPE poi_role AS ENUM ('meal','accommodation','attraction');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'theme_key') THEN
    CREATE TYPE theme_key AS ENUM (
      'religious_sites',
      'adventure',
      'art_museums',
      'family',
      'nature',
      'nightlife',
      'relax',
      'shopping',
      'cultural_history',
      'food_culinary'
    );
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'admin_kind') THEN
    CREATE TYPE admin_kind AS ENUM ('country','region','city','planning_area','district','neighborhood');
  END IF;
END$$;
