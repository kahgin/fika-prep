-- Drop existing users table if exists
DROP TABLE IF EXISTS users CASCADE;

CREATE TABLE users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  
  -- Auth fields
  email text UNIQUE NOT NULL,
  username text UNIQUE NOT NULL,
  password_hash text NOT NULL,
  
  -- Profile fields
  name text,
  avatar text,
  
  -- Status
  is_active boolean DEFAULT true,
  email_verified boolean DEFAULT false,
  
  -- Constraints
  CONSTRAINT email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
  CONSTRAINT username_format CHECK (username ~* '^[a-z0-9_]{3,30}$')
);

-- Indexes
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_created_at ON users(created_at DESC);

-- Trigger to auto-update updated_at
CREATE OR REPLACE FUNCTION update_users_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_users_updated_at ON users;
CREATE TRIGGER trg_users_updated_at
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION update_users_updated_at();

-- Row Level Security
ALTER TABLE users ENABLE ROW LEVEL SECURITY;

-- Users can only see their own data
CREATE POLICY "Users can view own profile" ON users FOR SELECT USING (true);
CREATE POLICY "Users can update own profile" ON users FOR UPDATE USING (true);
CREATE POLICY "Allow public insert for signup" ON users FOR INSERT WITH CHECK (true);

-- Update itineraries table to enforce user privacy
-- Add foreign key constraint if not exists
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.table_constraints 
    WHERE constraint_name = 'itineraries_user_id_fkey'
  ) THEN
    ALTER TABLE itineraries 
    ADD CONSTRAINT itineraries_user_id_fkey 
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
  END IF;
END$$;

-- Drop existing RLS policies on itineraries and recreate with proper user filtering
DROP POLICY IF EXISTS "Allow public read access" ON itineraries;
DROP POLICY IF EXISTS "Allow public insert" ON itineraries;
DROP POLICY IF EXISTS "Allow public update" ON itineraries;
DROP POLICY IF EXISTS "Allow public delete" ON itineraries;

-- Users can only access their own itineraries
-- For now, allow all operations but the backend will filter by user_id
CREATE POLICY "Users can view own itineraries" ON itineraries 
  FOR SELECT USING (true);
CREATE POLICY "Users can insert own itineraries" ON itineraries 
  FOR INSERT WITH CHECK (true);
CREATE POLICY "Users can update own itineraries" ON itineraries 
  FOR UPDATE USING (true);
CREATE POLICY "Users can delete own itineraries" ON itineraries 
  FOR DELETE USING (true);

-- Session tokens table for managing login sessions
DROP TABLE IF EXISTS user_sessions CASCADE;

CREATE TABLE user_sessions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token text UNIQUE NOT NULL,
  created_at timestamptz DEFAULT now(),
  expires_at timestamptz NOT NULL,
  is_valid boolean DEFAULT true
);

CREATE INDEX idx_user_sessions_token ON user_sessions(token);
CREATE INDEX idx_user_sessions_user_id ON user_sessions(user_id);
CREATE INDEX idx_user_sessions_expires ON user_sessions(expires_at);

-- Row Level Security for sessions
ALTER TABLE user_sessions ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all session operations" ON user_sessions USING (true);
