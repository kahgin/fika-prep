-- ============================================================================
-- Itinerary SQL Functions for Supabase
-- ============================================================================
-- These functions handle itinerary CRUD with proper ownership verification.
-- All functions follow the rpc_<action>_<entity> naming convention.
--
-- Functions:
--   rpc_get_itinerary_for_user    - Get itinerary with ownership check
--   rpc_update_itinerary_meta     - Update meta fields with ownership check
--   rpc_update_itinerary_plan     - Update plan with ownership check
--   rpc_claim_itinerary           - Claim an orphaned itinerary
--   rpc_delete_itinerary_for_user - Soft-delete with ownership check
--   rpc_list_user_itineraries     - List user's itineraries with pagination
--
-- Upload: python src/run_sql.py sql/itinerary_functions.sql
-- ============================================================================

-- Drop old function names if they exist (cleanup)
DROP FUNCTION IF EXISTS rpc_get_itinerary_for_user;
DROP FUNCTION IF EXISTS rpc_update_itinerary_meta;
DROP FUNCTION IF EXISTS rpc_update_itinerary_plan;
DROP FUNCTION IF EXISTS rpc_claim_itinerary;
DROP FUNCTION IF EXISTS rpc_delete_itinerary_for_user;
DROP FUNCTION IF EXISTS rpc_list_user_itineraries;


-- ----------------------------------------------------------------------------
-- Function: rpc_get_itinerary_for_user
-- ----------------------------------------------------------------------------
-- Retrieves an itinerary with ownership verification.
--
-- Rules:
-- 1. Authenticated users can access their own itineraries (user_id matches)
-- 2. Authenticated users can access orphaned itineraries (user_id is NULL)
-- 3. Guests (NULL user) can access orphaned itineraries only
-- 4. No one can access another user's itinerary
--
-- Returns access_status: 'ok', 'not_found', 'forbidden'
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION rpc_get_itinerary_for_user(
    p_itinerary_id UUID,
    p_user_id UUID DEFAULT NULL
)
RETURNS TABLE (
    id UUID,
    title TEXT,
    destinations JSONB,
    dates JSONB,
    travelers JSONB,
    preferences JSONB,
    flags JSONB,
    hotels JSONB,
    mandatory_pois JSONB,
    ideas JSONB,
    plan JSONB,
    user_id UUID,
    status TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    access_status TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_itinerary RECORD;
BEGIN
    -- Try to find the itinerary
    SELECT * INTO v_itinerary
    FROM itineraries i
    WHERE i.id = p_itinerary_id;

    -- Not found
    IF NOT FOUND THEN
        RETURN QUERY SELECT
            NULL::UUID, NULL::TEXT, NULL::JSONB, NULL::JSONB, NULL::JSONB,
            NULL::JSONB, NULL::JSONB, NULL::JSONB, NULL::JSONB, NULL::JSONB,
            NULL::JSONB, NULL::UUID, NULL::TEXT, NULL::TIMESTAMPTZ, NULL::TIMESTAMPTZ,
            'not_found'::TEXT;
        RETURN;
    END IF;

    -- Check ownership rules
    IF v_itinerary.user_id IS NOT NULL AND v_itinerary.user_id != p_user_id THEN
        RETURN QUERY SELECT
            NULL::UUID, NULL::TEXT, NULL::JSONB, NULL::JSONB, NULL::JSONB,
            NULL::JSONB, NULL::JSONB, NULL::JSONB, NULL::JSONB, NULL::JSONB,
            NULL::JSONB, NULL::UUID, NULL::TEXT, NULL::TIMESTAMPTZ, NULL::TIMESTAMPTZ,
            'forbidden'::TEXT;
        RETURN;
    END IF;

    -- Access granted - return the itinerary
    RETURN QUERY SELECT
        v_itinerary.id,
        v_itinerary.title,
        v_itinerary.destinations,
        v_itinerary.dates,
        v_itinerary.travelers,
        v_itinerary.preferences,
        v_itinerary.flags,
        v_itinerary.hotels,
        v_itinerary.mandatory_pois,
        v_itinerary.ideas,
        v_itinerary.plan,
        v_itinerary.user_id,
        v_itinerary.status,
        v_itinerary.created_at,
        v_itinerary.updated_at,
        'ok'::TEXT;
END;
$$;


-- ----------------------------------------------------------------------------
-- Function: rpc_update_itinerary_meta
-- ----------------------------------------------------------------------------
-- Updates itinerary metadata fields with ownership verification.
-- Only updates fields that are provided (not NULL).
--
-- Returns: success (bool), error_code ('ok', 'not_found', 'forbidden')
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION rpc_update_itinerary_meta(
    p_itinerary_id UUID,
    p_user_id UUID DEFAULT NULL,
    p_title TEXT DEFAULT NULL,
    p_dates JSONB DEFAULT NULL,
    p_travelers JSONB DEFAULT NULL,
    p_preferences JSONB DEFAULT NULL,
    p_flags JSONB DEFAULT NULL,
    p_hotels JSONB DEFAULT NULL,
    p_mandatory_pois JSONB DEFAULT NULL,
    p_ideas JSONB DEFAULT NULL
)
RETURNS TABLE (
    success BOOLEAN,
    error_code TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_owner UUID;
BEGIN
    -- Get current owner
    SELECT user_id INTO v_owner
    FROM itineraries
    WHERE id = p_itinerary_id;

    IF NOT FOUND THEN
        RETURN QUERY SELECT FALSE, 'not_found'::TEXT;
        RETURN;
    END IF;

    IF v_owner IS NOT NULL AND v_owner != p_user_id THEN
        RETURN QUERY SELECT FALSE, 'forbidden'::TEXT;
        RETURN;
    END IF;

    -- Update the itinerary (COALESCE keeps existing value if param is NULL)
    UPDATE itineraries SET
        title = COALESCE(p_title, title),
        dates = COALESCE(p_dates, dates),
        travelers = COALESCE(p_travelers, travelers),
        preferences = COALESCE(p_preferences, preferences),
        flags = COALESCE(p_flags, flags),
        hotels = COALESCE(p_hotels, hotels),
        mandatory_pois = COALESCE(p_mandatory_pois, mandatory_pois),
        ideas = COALESCE(p_ideas, ideas),
        updated_at = NOW()
    WHERE id = p_itinerary_id;

    RETURN QUERY SELECT TRUE, 'ok'::TEXT;
END;
$$;


-- ----------------------------------------------------------------------------
-- Function: rpc_update_itinerary_plan
-- ----------------------------------------------------------------------------
-- Updates the itinerary plan with ownership verification.
--
-- Returns: success (bool), error_code ('ok', 'not_found', 'forbidden')
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION rpc_update_itinerary_plan(
    p_itinerary_id UUID,
    p_user_id UUID DEFAULT NULL,
    p_plan JSONB DEFAULT NULL
)
RETURNS TABLE (
    success BOOLEAN,
    error_code TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_owner UUID;
BEGIN
    SELECT user_id INTO v_owner
    FROM itineraries
    WHERE id = p_itinerary_id;

    IF NOT FOUND THEN
        RETURN QUERY SELECT FALSE, 'not_found'::TEXT;
        RETURN;
    END IF;

    IF v_owner IS NOT NULL AND v_owner != p_user_id THEN
        RETURN QUERY SELECT FALSE, 'forbidden'::TEXT;
        RETURN;
    END IF;

    UPDATE itineraries SET
        plan = p_plan,
        updated_at = NOW()
    WHERE id = p_itinerary_id;

    RETURN QUERY SELECT TRUE, 'ok'::TEXT;
END;
$$;


-- ----------------------------------------------------------------------------
-- Function: rpc_claim_itinerary
-- ----------------------------------------------------------------------------
-- Claims an orphaned itinerary for a user (sets user_id).
-- Only works if the itinerary has no current owner.
--
-- Returns: success (bool), error_code ('ok', 'already_owned', 'not_found', 'forbidden', 'invalid_user')
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION rpc_claim_itinerary(
    p_itinerary_id UUID,
    p_user_id UUID
)
RETURNS TABLE (
    success BOOLEAN,
    error_code TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_owner UUID;
BEGIN
    IF p_user_id IS NULL THEN
        RETURN QUERY SELECT FALSE, 'invalid_user'::TEXT;
        RETURN;
    END IF;

    SELECT user_id INTO v_owner
    FROM itineraries
    WHERE id = p_itinerary_id;

    IF NOT FOUND THEN
        RETURN QUERY SELECT FALSE, 'not_found'::TEXT;
        RETURN;
    END IF;

    -- Can only claim orphaned itineraries
    IF v_owner IS NOT NULL THEN
        IF v_owner = p_user_id THEN
            RETURN QUERY SELECT TRUE, 'already_owned'::TEXT;
        ELSE
            RETURN QUERY SELECT FALSE, 'forbidden'::TEXT;
        END IF;
        RETURN;
    END IF;

    -- Claim the itinerary
    UPDATE itineraries SET
        user_id = p_user_id,
        updated_at = NOW()
    WHERE id = p_itinerary_id;

    RETURN QUERY SELECT TRUE, 'ok'::TEXT;
END;
$$;


-- ----------------------------------------------------------------------------
-- Function: rpc_delete_itinerary_for_user
-- ----------------------------------------------------------------------------
-- Soft-deletes an itinerary (sets status to 'deleted') with ownership check.
--
-- Returns: success (bool), error_code ('ok', 'not_found', 'forbidden')
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION rpc_delete_itinerary_for_user(
    p_itinerary_id UUID,
    p_user_id UUID DEFAULT NULL
)
RETURNS TABLE (
    success BOOLEAN,
    error_code TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_owner UUID;
BEGIN
    SELECT user_id INTO v_owner
    FROM itineraries
    WHERE id = p_itinerary_id;

    IF NOT FOUND THEN
        RETURN QUERY SELECT FALSE, 'not_found'::TEXT;
        RETURN;
    END IF;

    IF v_owner IS NOT NULL AND v_owner != p_user_id THEN
        RETURN QUERY SELECT FALSE, 'forbidden'::TEXT;
        RETURN;
    END IF;

    UPDATE itineraries SET
        status = 'deleted',
        updated_at = NOW()
    WHERE id = p_itinerary_id;

    RETURN QUERY SELECT TRUE, 'ok'::TEXT;
END;
$$;


-- ----------------------------------------------------------------------------
-- Function: rpc_list_user_itineraries
-- ----------------------------------------------------------------------------
-- Lists itineraries for a user with pagination.
--
-- Returns: id, title, destinations, dates, status, created_at, updated_at, total_count
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION rpc_list_user_itineraries(
    p_user_id UUID,
    p_status TEXT DEFAULT 'active',
    p_limit INT DEFAULT 20,
    p_offset INT DEFAULT 0
)
RETURNS TABLE (
    id UUID,
    title TEXT,
    destinations JSONB,
    dates JSONB,
    status TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    total_count BIGINT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_total BIGINT;
BEGIN
    -- Get total count
    SELECT COUNT(*) INTO v_total
    FROM itineraries i
    WHERE i.user_id = p_user_id
      AND (p_status IS NULL OR i.status = p_status);

    -- Return paginated results
    RETURN QUERY
    SELECT
        i.id,
        i.title,
        i.destinations,
        i.dates,
        i.status,
        i.created_at,
        i.updated_at,
        v_total
    FROM itineraries i
    WHERE i.user_id = p_user_id
      AND (p_status IS NULL OR i.status = p_status)
    ORDER BY i.updated_at DESC
    LIMIT p_limit
    OFFSET p_offset;
END;
$$;


-- ============================================================================
-- Grant execute permissions
-- ============================================================================
GRANT EXECUTE ON FUNCTION rpc_get_itinerary_for_user TO authenticated, anon;
GRANT EXECUTE ON FUNCTION rpc_update_itinerary_meta TO authenticated, anon;
GRANT EXECUTE ON FUNCTION rpc_update_itinerary_plan TO authenticated, anon;
GRANT EXECUTE ON FUNCTION rpc_claim_itinerary TO authenticated;
GRANT EXECUTE ON FUNCTION rpc_delete_itinerary_for_user TO authenticated, anon;
GRANT EXECUTE ON FUNCTION rpc_list_user_itineraries TO authenticated;
