-- Layer 0: Multi-tenancy
-- Creates tenants table, adds tenant_id to all 38 tables, enables RLS

BEGIN;

-- 1. Tenants table
CREATE TABLE IF NOT EXISTS tenants (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug        TEXT UNIQUE NOT NULL,
    name        TEXT NOT NULL,
    tier        TEXT NOT NULL DEFAULT 'free' CHECK (tier IN ('free', 'paid', 'self_hosted', 'enterprise')),
    api_key     TEXT UNIQUE NOT NULL DEFAULT 'hx_' || encode(gen_random_bytes(24), 'hex'),
    admin_key   TEXT UNIQUE NOT NULL DEFAULT 'hxa_' || encode(gen_random_bytes(24), 'hex'),
    is_active   BOOLEAN NOT NULL DEFAULT true,
    settings    JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. System tenant (fixed UUID for default foreign keys)
INSERT INTO tenants (id, slug, name, tier, api_key, admin_key)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    'system',
    'System (Ryan)',
    'self_hosted',
    'hx_system_00000000000000000000000000000000000000000000000000',
    'hxa_system_0000000000000000000000000000000000000000000000000'
)
ON CONFLICT (id) DO NOTHING;

-- 3. Add tenant_id to all 38 tables
DO $$
DECLARE
    tbl TEXT;
    tables TEXT[] := ARRAY[
        'anomalies','atoms','compression_log','compression_profiles',
        'conventions','decisions','dictionary_versions','embeddings',
        'entities','exchanges','expressions','intake_hashes',
        'kb_documents','kg_mentions','kg_relationships','membrain_events',
        'membrain_users','meta_events','meta_namespaces','molecules',
        'nodes','nudges','observer_actions','observer_exchanges',
        'observer_facts','observer_file_captures','observer_sequences',
        'observer_session_tokens','organisms','project_state','queue',
        'runbook_pages','sessions','shard_diffs','snapshot_queue',
        'snapshots','structured_archive','type_registry'
    ];
BEGIN
    FOREACH tbl IN ARRAY tables LOOP
        -- Add column if not exists
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = tbl AND column_name = 'tenant_id'
        ) THEN
            EXECUTE format(
                'ALTER TABLE %I ADD COLUMN tenant_id UUID NOT NULL DEFAULT %L REFERENCES tenants(id)',
                tbl,
                '00000000-0000-0000-0000-000000000001'
            );
            RAISE NOTICE 'Added tenant_id to %', tbl;
        END IF;
    END LOOP;
END;
$$;

-- 4. Indexes on tenant_id for all tables
DO $$
DECLARE
    tbl TEXT;
    tables TEXT[] := ARRAY[
        'anomalies','atoms','compression_log','compression_profiles',
        'conventions','decisions','dictionary_versions','embeddings',
        'entities','exchanges','expressions','intake_hashes',
        'kb_documents','kg_mentions','kg_relationships','membrain_events',
        'membrain_users','meta_events','meta_namespaces','molecules',
        'nodes','nudges','observer_actions','observer_exchanges',
        'observer_facts','observer_file_captures','observer_sequences',
        'observer_session_tokens','organisms','project_state','queue',
        'runbook_pages','sessions','shard_diffs','snapshot_queue',
        'snapshots','structured_archive','type_registry'
    ];
BEGIN
    FOREACH tbl IN ARRAY tables LOOP
        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS idx_%s_tenant_id ON %I (tenant_id)',
            tbl, tbl
        );
    END LOOP;
END;
$$;

-- 5. Enable RLS on all 38 tables
DO $$
DECLARE
    tbl TEXT;
    tables TEXT[] := ARRAY[
        'anomalies','atoms','compression_log','compression_profiles',
        'conventions','decisions','dictionary_versions','embeddings',
        'entities','exchanges','expressions','intake_hashes',
        'kb_documents','kg_mentions','kg_relationships','membrain_events',
        'membrain_users','meta_events','meta_namespaces','molecules',
        'nodes','nudges','observer_actions','observer_exchanges',
        'observer_facts','observer_file_captures','observer_sequences',
        'observer_session_tokens','organisms','project_state','queue',
        'runbook_pages','sessions','shard_diffs','snapshot_queue',
        'snapshots','structured_archive','type_registry'
    ];
BEGIN
    FOREACH tbl IN ARRAY tables LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
        EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
        -- Allow helix superuser to bypass
        EXECUTE format('ALTER TABLE %I NO FORCE ROW LEVEL SECURITY', tbl);
        -- Policy: tenant sees own rows OR system tenant sees all
        EXECUTE format(
            'DROP POLICY IF EXISTS tenant_isolation ON %I',
            tbl
        );
        EXECUTE format(
            $pol$CREATE POLICY tenant_isolation ON %I
                USING (
                    tenant_id::text = current_setting(''app.tenant_id'', true)
                    OR current_setting(''app.tenant_id'', true) = ''00000000-0000-0000-0000-000000000001''
                    OR current_setting(''app.tenant_id'', true) = ''''''
                )
            $pol$,
            tbl
        );
    END LOOP;
END;
$$;

-- 6. API key lookup fn (used by Helix middleware)
CREATE OR REPLACE FUNCTION get_tenant_by_api_key(p_key TEXT)
RETURNS TABLE(id UUID, slug TEXT, tier TEXT, is_active BOOLEAN) AS $$
    SELECT id, slug, tier, is_active FROM tenants
    WHERE api_key = p_key AND is_active = true
    LIMIT 1;
$$ LANGUAGE sql SECURITY DEFINER;

COMMIT;
