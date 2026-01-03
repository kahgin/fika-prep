-- Itineraries table for storing trip plans
DROP TABLE IF EXISTS itineraries CASCADE;

CREATE TABLE itineraries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  
  -- User reference (optional, for future auth integration)
  user_id uuid,
  
  -- Meta information (stored as JSONB for flexibility)
  title text,
  destinations jsonb DEFAULT '[]'::jsonb,
  dates jsonb DEFAULT '{}'::jsonb,
  travelers jsonb DEFAULT '{}'::jsonb,
  preferences jsonb DEFAULT '{}'::jsonb,
  flags jsonb DEFAULT '{}'::jsonb,
  hotels jsonb DEFAULT '[]'::jsonb,
  mandatory_pois jsonb DEFAULT '[]'::jsonb,
  ideas jsonb DEFAULT '[]'::jsonb,
  
  -- Plan data (the actual itinerary)
  plan jsonb DEFAULT '{}'::jsonb,
  
  -- Status
  status text DEFAULT 'active'
);

-- Indexes
CREATE INDEX idx_itineraries_user_id ON itineraries(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_itineraries_created_at ON itineraries(created_at DESC);
CREATE INDEX idx_itineraries_updated_at ON itineraries(updated_at DESC);
CREATE INDEX idx_itineraries_status ON itineraries(status);
CREATE INDEX idx_itineraries_destinations ON itineraries USING GIN (destinations);

-- Trigger to auto-update updated_at
CREATE OR REPLACE FUNCTION update_itineraries_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_itineraries_updated_at ON itineraries;
CREATE TRIGGER trg_itineraries_updated_at
BEFORE UPDATE ON itineraries
FOR EACH ROW
EXECUTE FUNCTION update_itineraries_updated_at();

-- Row Level Security
ALTER TABLE itineraries ENABLE ROW LEVEL SECURITY;

-- Allow public read access for now (can be restricted later with auth)
CREATE POLICY "Allow public read access" ON itineraries FOR SELECT USING (true);
CREATE POLICY "Allow public insert" ON itineraries FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow public update" ON itineraries FOR UPDATE USING (true);
CREATE POLICY "Allow public delete" ON itineraries FOR DELETE USING (true);

-- RPC Functions for CRUD operations

-- Create/Insert itinerary
DROP FUNCTION IF EXISTS rpc_create_itinerary;
CREATE OR REPLACE FUNCTION rpc_create_itinerary(
  p_id uuid DEFAULT NULL,
  p_title text DEFAULT NULL,
  p_destinations jsonb DEFAULT '[]'::jsonb,
  p_dates jsonb DEFAULT '{}'::jsonb,
  p_travelers jsonb DEFAULT '{}'::jsonb,
  p_preferences jsonb DEFAULT '{}'::jsonb,
  p_flags jsonb DEFAULT '{}'::jsonb,
  p_hotels jsonb DEFAULT '[]'::jsonb,
  p_mandatory_pois jsonb DEFAULT '[]'::jsonb,
  p_ideas jsonb DEFAULT '[]'::jsonb,
  p_plan jsonb DEFAULT '{}'::jsonb,
  p_user_id uuid DEFAULT NULL
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_id uuid;
BEGIN
  v_id := COALESCE(p_id, gen_random_uuid());
  
  INSERT INTO itineraries (
    id, title, destinations, dates, travelers, 
    preferences, flags, hotels, mandatory_pois, ideas, plan, user_id
  )
  VALUES (
    v_id, p_title, p_destinations, p_dates, p_travelers,
    p_preferences, p_flags, p_hotels, p_mandatory_pois, p_ideas, p_plan, p_user_id
  );
  
  RETURN v_id;
END;
$$;

-- Get itinerary by ID
DROP FUNCTION IF EXISTS rpc_get_itinerary;
CREATE OR REPLACE FUNCTION rpc_get_itinerary(p_id uuid)
RETURNS TABLE (
  id uuid,
  created_at timestamptz,
  updated_at timestamptz,
  user_id uuid,
  title text,
  destinations jsonb,
  dates jsonb,
  travelers jsonb,
  preferences jsonb,
  flags jsonb,
  hotels jsonb,
  mandatory_pois jsonb,
  ideas jsonb,
  plan jsonb,
  status text
)
LANGUAGE plpgsql
STABLE
SET search_path = public
AS $$
BEGIN
  RETURN QUERY
  SELECT 
    i.id,
    i.created_at,
    i.updated_at,
    i.user_id,
    i.title,
    i.destinations,
    i.dates,
    i.travelers,
    i.preferences,
    i.flags,
    i.hotels,
    i.mandatory_pois,
    i.ideas,
    i.plan,
    i.status
  FROM itineraries i
  WHERE i.id = p_id;
END;
$$;

-- Update itinerary
DROP FUNCTION IF EXISTS rpc_update_itinerary;
CREATE OR REPLACE FUNCTION rpc_update_itinerary(
  p_id uuid,
  p_title text DEFAULT NULL,
  p_destinations jsonb DEFAULT NULL,
  p_dates jsonb DEFAULT NULL,
  p_travelers jsonb DEFAULT NULL,
  p_preferences jsonb DEFAULT NULL,
  p_flags jsonb DEFAULT NULL,
  p_hotels jsonb DEFAULT NULL,
  p_mandatory_pois jsonb DEFAULT NULL,
  p_ideas jsonb DEFAULT NULL,
  p_plan jsonb DEFAULT NULL,
  p_status text DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  UPDATE itineraries
  SET
    title = COALESCE(p_title, title),
    destinations = COALESCE(p_destinations, destinations),
    dates = COALESCE(p_dates, dates),
    travelers = COALESCE(p_travelers, travelers),
    preferences = COALESCE(p_preferences, preferences),
    flags = COALESCE(p_flags, flags),
    hotels = COALESCE(p_hotels, hotels),
    mandatory_pois = COALESCE(p_mandatory_pois, mandatory_pois),
    ideas = COALESCE(p_ideas, ideas),
    plan = COALESCE(p_plan, plan),
    status = COALESCE(p_status, status)
  WHERE id = p_id;
  
  RETURN FOUND;
END;
$$;

-- Delete itinerary
DROP FUNCTION IF EXISTS rpc_delete_itinerary;
CREATE OR REPLACE FUNCTION rpc_delete_itinerary(p_id uuid)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  DELETE FROM itineraries WHERE id = p_id;
  RETURN FOUND;
END;
$$;

-- List itineraries (with pagination)
DROP FUNCTION IF EXISTS rpc_list_itineraries;
CREATE OR REPLACE FUNCTION rpc_list_itineraries(
  p_user_id uuid DEFAULT NULL,
  p_status text DEFAULT 'active',
  p_limit integer DEFAULT 20,
  p_offset integer DEFAULT 0
)
RETURNS TABLE (
  id uuid,
  created_at timestamptz,
  updated_at timestamptz,
  title text,
  destinations jsonb,
  dates jsonb,
  status text,
  total_count bigint
)
LANGUAGE plpgsql
STABLE
SET search_path = public
AS $$
DECLARE
  v_total_count bigint;
BEGIN
  -- Get total count
  SELECT COUNT(*) INTO v_total_count
  FROM itineraries i
  WHERE (p_user_id IS NULL OR i.user_id = p_user_id)
    AND (p_status IS NULL OR i.status = p_status);
  
  RETURN QUERY
  SELECT 
    i.id,
    i.created_at,
    i.updated_at,
    i.title,
    i.destinations,
    i.dates,
    i.status,
    v_total_count
  FROM itineraries i
  WHERE (p_user_id IS NULL OR i.user_id = p_user_id)
    AND (p_status IS NULL OR i.status = p_status)
  ORDER BY i.updated_at DESC
  LIMIT p_limit OFFSET p_offset;
END;
$$;

-- Upsert itinerary (create or update)
DROP FUNCTION IF EXISTS rpc_upsert_itinerary;
CREATE OR REPLACE FUNCTION rpc_upsert_itinerary(
  p_id uuid,
  p_title text DEFAULT NULL,
  p_destinations jsonb DEFAULT NULL,
  p_dates jsonb DEFAULT NULL,
  p_travelers jsonb DEFAULT NULL,
  p_preferences jsonb DEFAULT NULL,
  p_flags jsonb DEFAULT NULL,
  p_hotels jsonb DEFAULT NULL,
  p_mandatory_pois jsonb DEFAULT NULL,
  p_ideas jsonb DEFAULT NULL,
  p_plan jsonb DEFAULT NULL,
  p_user_id uuid DEFAULT NULL
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  INSERT INTO itineraries (
    id, title, destinations, dates, travelers, 
    preferences, flags, hotels, mandatory_pois, ideas, plan, user_id
  )
  VALUES (
    p_id,
    COALESCE(p_title, ''),
    COALESCE(p_destinations, '[]'::jsonb),
    COALESCE(p_dates, '{}'::jsonb),
    COALESCE(p_travelers, '{}'::jsonb),
    COALESCE(p_preferences, '{}'::jsonb),
    COALESCE(p_flags, '{}'::jsonb),
    COALESCE(p_hotels, '[]'::jsonb),
    COALESCE(p_mandatory_pois, '[]'::jsonb),
    COALESCE(p_ideas, '[]'::jsonb),
    COALESCE(p_plan, '{}'::jsonb),
    p_user_id
  )
  ON CONFLICT (id) DO UPDATE
  SET
    title = COALESCE(EXCLUDED.title, itineraries.title),
    destinations = COALESCE(EXCLUDED.destinations, itineraries.destinations),
    dates = COALESCE(EXCLUDED.dates, itineraries.dates),
    travelers = COALESCE(EXCLUDED.travelers, itineraries.travelers),
    preferences = COALESCE(EXCLUDED.preferences, itineraries.preferences),
    flags = COALESCE(EXCLUDED.flags, itineraries.flags),
    hotels = COALESCE(EXCLUDED.hotels, itineraries.hotels),
    mandatory_pois = COALESCE(EXCLUDED.mandatory_pois, itineraries.mandatory_pois),
    ideas = COALESCE(EXCLUDED.ideas, itineraries.ideas),
    plan = COALESCE(EXCLUDED.plan, itineraries.plan),
    updated_at = now();
  
  RETURN p_id;
END;
$$;
