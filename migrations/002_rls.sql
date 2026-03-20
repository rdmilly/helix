-- 002_rls.sql  Phase 8: Row-Level Security
-- Run as superuser (helix). helix_app is the non-privileged runtime role.
-- Existing data has user_id='system'; app connects as helix_app with
-- SET LOCAL helix.user_id = '<user>'  per connection.

-- ================================================================
-- 1. Create runtime role
-- ================================================================
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'helix_app') THEN
    CREATE ROLE helix_app LOGIN PASSWORD 'helix_app_9f3c2a1b';
  END IF;
END $$;

-- Grant connect + usage
GRANT CONNECT ON DATABASE helix TO helix_app;
GRANT USAGE ON SCHEMA public TO helix_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO helix_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO helix_app;
-- Future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO helix_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO helix_app;

-- ================================================================
-- 2. Enable RLS on core user-data tables
-- ================================================================
ALTER TABLE sessions          ENABLE ROW LEVEL SECURITY;
ALTER TABLE exchanges         ENABLE ROW LEVEL SECURITY;
ALTER TABLE entities          ENABLE ROW LEVEL SECURITY;
ALTER TABLE structured_archive ENABLE ROW LEVEL SECURITY;
ALTER TABLE embeddings        ENABLE ROW LEVEL SECURITY;
ALTER TABLE queue             ENABLE ROW LEVEL SECURITY;
ALTER TABLE observer_actions  ENABLE ROW LEVEL SECURITY;
ALTER TABLE kg_relationships  ENABLE ROW LEVEL SECURITY;
ALTER TABLE membrain_events   ENABLE ROW LEVEL SECURITY;

-- ================================================================
-- 3. Create policies
-- Policy logic:
--   user_id = current user setting  -> owns the row
--   user_id = 'system'             -> shared/admin data, visible to all
-- ================================================================

-- Helper: get current user id, default to 'system' if not set
-- This means unset connections see system data (safe default)

CREATE OR REPLACE FUNCTION helix_current_user() RETURNS text
  LANGUAGE sql STABLE
  AS $$ SELECT COALESCE(NULLIF(current_setting('helix.user_id', true), ''), 'system') $$;

-- Sessions
DROP POLICY IF EXISTS helix_sessions_policy ON sessions;
CREATE POLICY helix_sessions_policy ON sessions
  USING (user_id = helix_current_user() OR user_id = 'system');

-- Exchanges
DROP POLICY IF EXISTS helix_exchanges_policy ON exchanges;
CREATE POLICY helix_exchanges_policy ON exchanges
  USING (user_id = helix_current_user() OR user_id = 'system');

-- Entities
DROP POLICY IF EXISTS helix_entities_policy ON entities;
CREATE POLICY helix_entities_policy ON entities
  USING (user_id = helix_current_user() OR user_id = 'system');

-- Structured archive
DROP POLICY IF EXISTS helix_archive_policy ON structured_archive;
CREATE POLICY helix_archive_policy ON structured_archive
  USING (user_id = helix_current_user() OR user_id = 'system');

-- Embeddings
DROP POLICY IF EXISTS helix_embeddings_policy ON embeddings;
CREATE POLICY helix_embeddings_policy ON embeddings
  USING (user_id = helix_current_user() OR user_id = 'system');

-- Queue
DROP POLICY IF EXISTS helix_queue_policy ON queue;
CREATE POLICY helix_queue_policy ON queue
  USING (user_id = helix_current_user() OR user_id = 'system');

-- Observer actions
DROP POLICY IF EXISTS helix_observer_policy ON observer_actions;
CREATE POLICY helix_observer_policy ON observer_actions
  USING (user_id = helix_current_user() OR user_id = 'system');

-- KG relationships
DROP POLICY IF EXISTS helix_kg_policy ON kg_relationships;
CREATE POLICY helix_kg_policy ON kg_relationships
  USING (user_id = helix_current_user() OR user_id = 'system');

-- Membrain events
DROP POLICY IF EXISTS helix_membrain_policy ON membrain_events;
CREATE POLICY helix_membrain_policy ON membrain_events
  USING (user_id = helix_current_user() OR user_id = 'system');

-- ================================================================
-- 4. Ensure existing rows have user_id = 'system'
-- ================================================================
UPDATE sessions          SET user_id = 'system' WHERE user_id IS NULL OR user_id = '';
UPDATE exchanges         SET user_id = 'system' WHERE user_id IS NULL OR user_id = '';
UPDATE entities          SET user_id = 'system' WHERE user_id IS NULL OR user_id = '';
UPDATE structured_archive SET user_id = 'system' WHERE user_id IS NULL OR user_id = '';
UPDATE embeddings        SET user_id = 'system' WHERE user_id IS NULL OR user_id = '';
UPDATE queue             SET user_id = 'system' WHERE user_id IS NULL OR user_id = '';
UPDATE observer_actions  SET user_id = 'system' WHERE user_id IS NULL OR user_id = '';
UPDATE kg_relationships  SET user_id = 'system' WHERE user_id IS NULL OR user_id = '';
UPDATE membrain_events   SET user_id = 'system' WHERE user_id IS NULL OR user_id = '';

-- ================================================================
-- 5. Verify
-- ================================================================
SELECT tablename, rowsecurity
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN ('sessions','exchanges','entities','structured_archive',
                    'embeddings','queue','observer_actions','kg_relationships','membrain_events')
ORDER BY tablename;
