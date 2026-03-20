-- ============================================================
-- Helix Cortex — PostgreSQL Schema
-- Migrated from SQLite cortex.db
-- Phase 2 of Helix Intelligence Migration
-- ============================================================

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================================
-- SESSIONS
-- ============================================================
CREATE TABLE sessions (
    id           TEXT PRIMARY KEY,
    provider     TEXT,
    model        TEXT,
    summary      TEXT,
    significance FLOAT8 DEFAULT 0.0,
    tags_json    JSONB DEFAULT '[]',
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    meta         JSONB DEFAULT '{}',
    user_id      TEXT NOT NULL DEFAULT 'system'
);
CREATE INDEX idx_sessions_user_id    ON sessions(user_id);
CREATE INDEX idx_sessions_created_at ON sessions(created_at DESC);

-- ============================================================
-- EXCHANGES
-- ============================================================
CREATE TABLE exchanges (
    id                   TEXT PRIMARY KEY,
    session_id           TEXT NOT NULL,
    exchange_num         INTEGER DEFAULT 0,
    timestamp            TIMESTAMPTZ NOT NULL,
    exchange_type        TEXT DEFAULT 'discuss',
    project              TEXT DEFAULT '',
    domain               TEXT DEFAULT '',
    files_changed        JSONB DEFAULT '[]',
    services_changed     JSONB DEFAULT '[]',
    state_before         TEXT DEFAULT '',
    state_after          TEXT DEFAULT '',
    decision             TEXT DEFAULT '',
    reason               TEXT DEFAULT '',
    rejected_alternatives TEXT DEFAULT '',
    constraint_discovered TEXT DEFAULT '',
    failure              TEXT DEFAULT '',
    pattern              TEXT DEFAULT '',
    entities_mentioned   JSONB DEFAULT '[]',
    relationships_found  JSONB DEFAULT '[]',
    next_step            TEXT DEFAULT '',
    open_questions       JSONB DEFAULT '[]',
    confidence           FLOAT8 DEFAULT 0.7,
    session_summary      TEXT DEFAULT '',
    session_goals        JSONB DEFAULT '[]',
    actions_taken        JSONB DEFAULT '[]',
    skip                 INTEGER DEFAULT 0,
    tool_calls           INTEGER DEFAULT 0,
    tools_used           JSONB DEFAULT '[]',
    complexity           TEXT DEFAULT 'low',
    what_happened        TEXT DEFAULT '',
    notes                TEXT DEFAULT '',
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    user_id              TEXT NOT NULL DEFAULT 'system'
);
CREATE INDEX idx_exchanges_session_id  ON exchanges(session_id);
CREATE INDEX idx_exchanges_user_id     ON exchanges(user_id);
CREATE INDEX idx_exchanges_created_at  ON exchanges(created_at DESC);
CREATE INDEX idx_exchanges_project     ON exchanges(project);

ALTER TABLE exchanges ADD COLUMN search_vector tsvector;
CREATE INDEX idx_exchanges_fts ON exchanges USING GIN(search_vector);
CREATE FUNCTION exchanges_fts_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.what_happened,'')), 'A') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.decision,'')), 'B') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.reason,'')), 'C') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.failure,'')), 'C') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.pattern,'')), 'C') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.notes,'')), 'D');
    RETURN NEW;
END;
$$;
CREATE TRIGGER trg_exchanges_fts
    BEFORE INSERT OR UPDATE ON exchanges
    FOR EACH ROW EXECUTE FUNCTION exchanges_fts_update();

-- ============================================================
-- ENTITIES
-- ============================================================
CREATE TABLE entities (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    entity_type     TEXT,
    attributes_json JSONB DEFAULT '{}',
    first_seen      TIMESTAMPTZ DEFAULT NOW(),
    last_seen       TIMESTAMPTZ DEFAULT NOW(),
    meta            JSONB DEFAULT '{}',
    description     TEXT DEFAULT '',
    mention_count   INTEGER DEFAULT 0,
    user_id         TEXT NOT NULL DEFAULT 'system'
);
CREATE INDEX idx_entities_name      ON entities(name);
CREATE INDEX idx_entities_type      ON entities(entity_type);
CREATE INDEX idx_entities_user_id   ON entities(user_id);

ALTER TABLE entities ADD COLUMN search_vector tsvector;
CREATE INDEX idx_entities_fts ON entities USING GIN(search_vector);
CREATE FUNCTION entities_fts_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.name,'')), 'A') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.entity_type,'')), 'B') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.description,'')), 'C');
    RETURN NEW;
END;
$$;
CREATE TRIGGER trg_entities_fts
    BEFORE INSERT OR UPDATE ON entities
    FOR EACH ROW EXECUTE FUNCTION entities_fts_update();

-- ============================================================
-- KG_RELATIONSHIPS
-- ============================================================
CREATE TABLE kg_relationships (
    id            BIGSERIAL PRIMARY KEY,
    source_name   TEXT NOT NULL,
    target_name   TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    description   TEXT DEFAULT '',
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    session_id    TEXT DEFAULT '',
    user_id       TEXT NOT NULL DEFAULT 'system',
    UNIQUE(source_name, target_name, relation_type)
);
CREATE INDEX idx_kg_rel_source  ON kg_relationships(source_name);
CREATE INDEX idx_kg_rel_target  ON kg_relationships(target_name);
CREATE INDEX idx_kg_rel_user_id ON kg_relationships(user_id);

-- ============================================================
-- KG_MENTIONS
-- ============================================================
CREATE TABLE kg_mentions (
    id           BIGSERIAL PRIMARY KEY,
    entity_name  TEXT NOT NULL,
    session_id   TEXT DEFAULT '',
    context      TEXT DEFAULT '',
    mentioned_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_kg_mentions_entity ON kg_mentions(entity_name);

-- ============================================================
-- DECISIONS
-- ============================================================
CREATE TABLE decisions (
    id         TEXT PRIMARY KEY,
    session_id TEXT REFERENCES sessions(id) ON DELETE SET NULL,
    decision   TEXT NOT NULL,
    rationale  TEXT,
    project    TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    meta       JSONB DEFAULT '{}'
);
CREATE INDEX idx_decisions_session_id ON decisions(session_id);
CREATE INDEX idx_decisions_project    ON decisions(project);

-- ============================================================
-- QUEUE
-- ============================================================
CREATE TABLE queue (
    id           TEXT PRIMARY KEY,
    intake_type  TEXT NOT NULL,
    content_type TEXT,
    payload      JSONB NOT NULL,
    status       TEXT DEFAULT 'pending',
    priority     INTEGER DEFAULT 0,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error        TEXT,
    attempts     INTEGER DEFAULT 0,
    meta         JSONB DEFAULT '{}',
    user_id      TEXT NOT NULL DEFAULT 'system'
);
CREATE INDEX idx_queue_status    ON queue(status);
CREATE INDEX idx_queue_priority  ON queue(priority DESC, created_at ASC);
CREATE INDEX idx_queue_user_id   ON queue(user_id);

-- ============================================================
-- STRUCTURED_ARCHIVE
-- ============================================================
CREATE TABLE structured_archive (
    id            TEXT PRIMARY KEY,
    collection    TEXT NOT NULL,
    content       TEXT NOT NULL,
    metadata_json JSONB DEFAULT '{}',
    session_id    TEXT DEFAULT '',
    timestamp     TEXT DEFAULT '',
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    user_id       TEXT NOT NULL DEFAULT 'system'
);
CREATE INDEX idx_structured_collection ON structured_archive(collection);
CREATE INDEX idx_structured_user_id    ON structured_archive(user_id);

ALTER TABLE structured_archive ADD COLUMN search_vector tsvector;
CREATE INDEX idx_structured_fts ON structured_archive USING GIN(search_vector);
CREATE FUNCTION structured_fts_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.content,'')), 'A') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.collection,'')), 'B') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.session_id,'')), 'D');
    RETURN NEW;
END;
$$;
CREATE TRIGGER trg_structured_fts
    BEFORE INSERT OR UPDATE ON structured_archive
    FOR EACH ROW EXECUTE FUNCTION structured_fts_update();

-- ============================================================
-- KB_DOCUMENTS
-- ============================================================
CREATE TABLE kb_documents (
    id           TEXT PRIMARY KEY,
    source       TEXT NOT NULL,
    path         TEXT NOT NULL,
    title        TEXT DEFAULT '',
    content      TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    size_bytes   INTEGER DEFAULT 0,
    indexed_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source, path)
);
CREATE INDEX idx_kb_source ON kb_documents(source);

ALTER TABLE kb_documents ADD COLUMN search_vector tsvector;
CREATE INDEX idx_kb_fts ON kb_documents USING GIN(search_vector);
CREATE FUNCTION kb_fts_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.title,'')), 'A') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.content,'')), 'B') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.source,'')), 'C') ||
        setweight(to_tsvector('pg_catalog.english', coalesce(NEW.path,'')), 'D');
    RETURN NEW;
END;
$$;
CREATE TRIGGER trg_kb_fts
    BEFORE INSERT OR UPDATE ON kb_documents
    FOR EACH ROW EXECUTE FUNCTION kb_fts_update();

-- ============================================================
-- MEMBRAIN_EVENTS
-- ============================================================
CREATE TABLE membrain_events (
    id         BIGSERIAL PRIMARY KEY,
    user_id    TEXT NOT NULL,
    event_type TEXT NOT NULL,
    meta       JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_membrain_events_user_id ON membrain_events(user_id);
CREATE INDEX idx_membrain_events_type    ON membrain_events(event_type);

-- ============================================================
-- MEMBRAIN_USERS
-- ============================================================
CREATE TABLE membrain_users (
    id              TEXT PRIMARY KEY,
    token_hash      TEXT UNIQUE NOT NULL,
    email           TEXT NOT NULL DEFAULT '',
    tier            TEXT NOT NULL DEFAULT 'paid',
    collection_name TEXT NOT NULL,
    revoked         INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    meta            JSONB NOT NULL DEFAULT '{}'
);

-- ============================================================
-- OBSERVER_ACTIONS
-- ============================================================
CREATE TABLE observer_actions (
    id              BIGSERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL,
    session_id      TEXT,
    sequence_num    INTEGER DEFAULT 0,
    tool_name       TEXT NOT NULL,
    server_name     TEXT,
    category        TEXT DEFAULT 'other',
    arguments_json  JSONB,
    result_summary  TEXT,
    has_file_content INTEGER DEFAULT 0,
    file_path       TEXT,
    file_size       INTEGER,
    duration_ms     INTEGER,
    error           INTEGER DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    user_id         TEXT NOT NULL DEFAULT 'system'
);
CREATE INDEX idx_observer_actions_session  ON observer_actions(session_id);
CREATE INDEX idx_observer_actions_tool     ON observer_actions(tool_name);
CREATE INDEX idx_observer_actions_user_id  ON observer_actions(user_id);
CREATE INDEX idx_observer_actions_ts       ON observer_actions(timestamp DESC);

-- ============================================================
-- OBSERVER_EXCHANGES
-- ============================================================
CREATE TABLE observer_exchanges (
    id              BIGSERIAL PRIMARY KEY,
    session_id      TEXT,
    prompt_preview  TEXT,
    response_preview TEXT,
    model           TEXT,
    tool_call_count INTEGER DEFAULT 0,
    timestamp       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_obs_exchanges_session ON observer_exchanges(session_id);

-- ============================================================
-- OBSERVER_FACTS
-- ============================================================
CREATE TABLE observer_facts (
    id          BIGSERIAL PRIMARY KEY,
    source_file TEXT,
    fact_type   TEXT NOT NULL,
    fact_key    TEXT NOT NULL,
    fact_value  TEXT NOT NULL,
    confidence  FLOAT8 DEFAULT 1.0,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(fact_type, fact_key, fact_value)
);

-- ============================================================
-- OBSERVER_FILE_CAPTURES
-- ============================================================
CREATE TABLE observer_file_captures (
    id                BIGSERIAL PRIMARY KEY,
    action_id         INTEGER,
    file_path         TEXT NOT NULL,
    content           TEXT NOT NULL,
    language          TEXT,
    content_type      TEXT DEFAULT 'code',
    char_count        INTEGER DEFAULT 0,
    scanned           INTEGER DEFAULT 0,
    indexed           INTEGER DEFAULT 0,
    facts_extracted   INTEGER DEFAULT 0,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- OBSERVER_SEQUENCES
-- ============================================================
CREATE TABLE observer_sequences (
    id               BIGSERIAL PRIMARY KEY,
    session_id       TEXT NOT NULL,
    tool_sequence    TEXT NOT NULL,
    sequence_hash    TEXT NOT NULL UNIQUE,
    length           INTEGER NOT NULL,
    first_seen       TIMESTAMPTZ NOT NULL,
    last_seen        TIMESTAMPTZ NOT NULL,
    occurrence_count INTEGER DEFAULT 1,
    category         TEXT DEFAULT 'detected'
);

-- ============================================================
-- OBSERVER_SESSION_TOKENS
-- ============================================================
CREATE TABLE observer_session_tokens (
    id             BIGSERIAL PRIMARY KEY,
    session_id     TEXT NOT NULL,
    exchange_num   INTEGER NOT NULL,
    tokens_in      INTEGER DEFAULT 0,
    tokens_out     INTEGER DEFAULT 0,
    tool_calls     INTEGER DEFAULT 0,
    cumulative_in  INTEGER DEFAULT 0,
    cumulative_out INTEGER DEFAULT 0,
    timestamp      TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_obs_tokens_session ON observer_session_tokens(session_id);

-- ============================================================
-- ATOMS
-- ============================================================
CREATE TABLE atoms (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    full_name       TEXT,
    code            TEXT NOT NULL,
    template        TEXT,
    parameters_json JSONB,
    structural_fp   TEXT,
    semantic_fp     TEXT,
    fp_version      TEXT DEFAULT 'v1',
    first_seen      TIMESTAMPTZ DEFAULT NOW(),
    last_seen       TIMESTAMPTZ DEFAULT NOW(),
    occurrence_count INTEGER DEFAULT 1,
    meta            JSONB DEFAULT '{}'
);
CREATE INDEX idx_atoms_name ON atoms(name);

-- ============================================================
-- MOLECULES
-- ============================================================
CREATE TABLE molecules (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    description         TEXT,
    atom_ids_json       JSONB,
    atom_names_json     JSONB,
    template            TEXT,
    co_occurrence_count INTEGER DEFAULT 0,
    first_seen          TIMESTAMPTZ DEFAULT NOW(),
    meta                JSONB DEFAULT '{}'
);

-- ============================================================
-- ORGANISMS
-- ============================================================
CREATE TABLE organisms (
    id                TEXT PRIMARY KEY,
    name              TEXT NOT NULL,
    description       TEXT,
    molecule_ids_json JSONB,
    template          TEXT,
    first_seen        TIMESTAMPTZ DEFAULT NOW(),
    meta              JSONB DEFAULT '{}'
);

-- ============================================================
-- EXPRESSIONS
-- ============================================================
CREATE TABLE expressions (
    id               TEXT PRIMARY KEY,
    archetype        TEXT NOT NULL,
    framework        TEXT NOT NULL,
    section          TEXT NOT NULL DEFAULT 'utility',
    skeleton         TEXT NOT NULL,
    parameter_map    JSONB DEFAULT '{}',
    observed_from    JSONB DEFAULT '[]',
    observed_count   INTEGER DEFAULT 1,
    confidence       FLOAT8 DEFAULT 0.5,
    meta             JSONB DEFAULT '{}',
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW(),
    structural_params JSONB DEFAULT '[]',
    generated_by     TEXT DEFAULT 'extracted',
    skeleton_version TEXT DEFAULT '1',
    UNIQUE(archetype, framework, section)
);

-- ============================================================
-- ANOMALIES
-- ============================================================
CREATE TABLE anomalies (
    id          TEXT PRIMARY KEY,
    type        TEXT,
    description TEXT NOT NULL,
    evidence    TEXT,
    severity    TEXT DEFAULT 'MEDIUM',
    state       TEXT DEFAULT 'active',
    session_id  TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    meta        JSONB DEFAULT '{}'
);

-- ============================================================
-- NUDGES
-- ============================================================
CREATE TABLE nudges (
    id          TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    category    TEXT,
    priority    TEXT DEFAULT 'MEDIUM',
    state       TEXT DEFAULT 'active',
    session_id  TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    meta        JSONB DEFAULT '{}'
);

-- ============================================================
-- CONVENTIONS
-- ============================================================
CREATE TABLE conventions (
    id          TEXT PRIMARY KEY,
    pattern     TEXT NOT NULL,
    description TEXT,
    confidence  FLOAT8 DEFAULT 0.0,
    occurrences INTEGER DEFAULT 0,
    scope       TEXT,
    first_seen  TIMESTAMPTZ DEFAULT NOW(),
    meta        JSONB DEFAULT '{}'
);

-- ============================================================
-- COMPRESSION_LOG
-- ============================================================
CREATE TABLE compression_log (
    id                      TEXT PRIMARY KEY,
    timestamp               TIMESTAMPTZ DEFAULT NOW(),
    provider                TEXT,
    model                   TEXT,
    conversation_id         TEXT,
    session_id              TEXT,
    tokens_original_in      INTEGER,
    tokens_compressed_in    INTEGER,
    tokens_original_out     INTEGER,
    tokens_compressed_out   INTEGER,
    compression_ratio_in    FLOAT8,
    compression_ratio_out   FLOAT8,
    layers                  JSONB DEFAULT '[]',
    pattern_ref_hits        INTEGER DEFAULT 0,
    dictionary_version      TEXT,
    tokenizer               TEXT,
    meta                    JSONB DEFAULT '{}'
);

-- ============================================================
-- COMPRESSION_PROFILES
-- ============================================================
CREATE TABLE compression_profiles (
    id                    BIGSERIAL PRIMARY KEY,
    role                  TEXT NOT NULL,
    phrase                TEXT NOT NULL,
    compressed            TEXT NOT NULL DEFAULT '',
    pattern_type          TEXT NOT NULL,
    stage                 TEXT NOT NULL DEFAULT 'candidate',
    frequency             INTEGER NOT NULL DEFAULT 0,
    session_count         INTEGER NOT NULL DEFAULT 0,
    tokens_saved_per_use  INTEGER NOT NULL DEFAULT 2,
    total_tokens_saved    INTEGER NOT NULL DEFAULT 0,
    first_seen            TIMESTAMPTZ NOT NULL,
    last_seen             TIMESTAMPTZ NOT NULL,
    last_promoted         TIMESTAMPTZ,
    last_analyzed         TIMESTAMPTZ NOT NULL,
    compression_assigned  INTEGER NOT NULL DEFAULT 0,
    UNIQUE(role, phrase)
);

-- ============================================================
-- DICTIONARY_VERSIONS
-- ============================================================
CREATE TABLE dictionary_versions (
    version      TEXT PRIMARY KEY,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    entries_count INTEGER,
    dictionary   JSONB NOT NULL,
    delta_from   TEXT,
    delta        JSONB
);

-- ============================================================
-- INTAKE_HASHES
-- ============================================================
CREATE TABLE intake_hashes (
    content_hash TEXT PRIMARY KEY,
    intake_type  TEXT,
    received_at  TIMESTAMPTZ DEFAULT NOW(),
    queue_id     TEXT
);

-- ============================================================
-- META_EVENTS
-- ============================================================
CREATE TABLE meta_events (
    id           TEXT PRIMARY KEY,
    target_table TEXT NOT NULL,
    target_id    TEXT NOT NULL,
    namespace    TEXT NOT NULL,
    action       TEXT NOT NULL,
    old_value    TEXT,
    new_value    TEXT,
    written_by   TEXT,
    timestamp    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_meta_events_target ON meta_events(target_table, target_id);

-- ============================================================
-- META_NAMESPACES
-- ============================================================
CREATE TABLE meta_namespaces (
    namespace       TEXT PRIMARY KEY,
    registered_by   TEXT NOT NULL,
    registered_at   TIMESTAMPTZ DEFAULT NOW(),
    fields_schema   JSONB,
    description     TEXT,
    applies_to      JSONB DEFAULT '["atoms"]',
    version         TEXT DEFAULT '1.0'
);

-- ============================================================
-- PROJECT_STATE
-- ============================================================
CREATE TABLE project_state (
    project    TEXT PRIMARY KEY,
    status     TEXT,
    one_liner  TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    meta       JSONB DEFAULT '{}'
);

-- ============================================================
-- RUNBOOK_PAGES
-- ============================================================
CREATE TABLE runbook_pages (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    description   TEXT NOT NULL DEFAULT '',
    category      TEXT NOT NULL DEFAULT 'reference',
    source_type   TEXT NOT NULL DEFAULT 'static',
    source_config JSONB NOT NULL DEFAULT '{}',
    triggers      JSONB NOT NULL DEFAULT '[]',
    priority      INTEGER NOT NULL DEFAULT 50,
    active        INTEGER NOT NULL DEFAULT 1,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);

-- ============================================================
-- SHARD_DIFFS
-- ============================================================
CREATE TABLE shard_diffs (
    id               BIGSERIAL PRIMARY KEY,
    path             TEXT NOT NULL,
    session_id       TEXT,
    lines_added      INTEGER DEFAULT 0,
    lines_removed    INTEGER DEFAULT 0,
    lines_unchanged  INTEGER DEFAULT 0,
    diff_text        TEXT,
    diff_type        TEXT DEFAULT 'delta',
    prev_chars       INTEGER DEFAULT 0,
    new_chars        INTEGER DEFAULT 0,
    created_at       TIMESTAMPTZ
);
CREATE INDEX idx_shard_diffs_path ON shard_diffs(path);

-- ============================================================
-- SNAPSHOT_QUEUE
-- ============================================================
CREATE TABLE snapshot_queue (
    id           TEXT PRIMARY KEY,
    target_table TEXT NOT NULL,
    target_id    TEXT NOT NULL,
    reason       TEXT DEFAULT 'manual',
    queued_at    TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

-- ============================================================
-- SNAPSHOTS
-- ============================================================
CREATE TABLE snapshots (
    id           TEXT PRIMARY KEY,
    target_table TEXT NOT NULL,
    target_id    TEXT NOT NULL,
    content      JSONB NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_snapshots_target ON snapshots(target_table, target_id);

-- ============================================================
-- TYPE_REGISTRY
-- ============================================================
CREATE TABLE type_registry (
    type_name      TEXT PRIMARY KEY,
    category       TEXT NOT NULL,
    handler        TEXT,
    registered_by  TEXT NOT NULL,
    registered_at  TIMESTAMPTZ DEFAULT NOW(),
    config         JSONB DEFAULT '{}',
    active         INTEGER DEFAULT 1
);

-- ============================================================
-- EMBEDDINGS  (new — replaces ChromaDB)
-- ============================================================
CREATE TABLE embeddings (
    id          TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_id   TEXT NOT NULL,
    content     TEXT NOT NULL,
    embedding   vector(1024),
    model       TEXT DEFAULT 'bge-large-en-v1.5',
    user_id     TEXT DEFAULT 'system',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_embeddings_source  ON embeddings(source_type, source_id);
CREATE INDEX idx_embeddings_user_id ON embeddings(user_id);
CREATE INDEX idx_embeddings_vector  ON embeddings USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

