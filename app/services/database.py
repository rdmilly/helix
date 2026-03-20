"""Database Service - SQLite with Epigenetic Schema

Initializes and manages Cortex database with full epigenetic architecture.
ALL tables have meta TEXT DEFAULT '{}' columns.
Algorithm versioning baked in (fp_version, dictionary_versions, etc).
"""
import sqlite3
import logging
from pathlib import Path
from typing import Optional
from contextlib import contextmanager
from config import DB_PATH

logger = logging.getLogger(__name__)


class Database:
    """SQLite database with epigenetic schema"""
    
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()
    
    @contextmanager
    def get_connection(self):
        """Get database connection context manager"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def _initialize_schema(self):
        """Initialize all tables with epigenetic schema"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # === CORE TABLES (DNA -- fixed identity) ===
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS atoms (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    full_name TEXT,
                    code TEXT NOT NULL,
                    template TEXT,
                    parameters_json TEXT,
                    structural_fp TEXT,
                    semantic_fp TEXT,
                    fp_version TEXT DEFAULT 'v1',
                    first_seen TEXT DEFAULT (datetime('now')),
                    last_seen TEXT DEFAULT (datetime('now')),
                    occurrence_count INTEGER DEFAULT 1,
                    meta TEXT DEFAULT '{}'
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS molecules (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    atom_ids_json TEXT,
                    atom_names_json TEXT,
                    template TEXT,
                    co_occurrence_count INTEGER DEFAULT 0,
                    first_seen TEXT DEFAULT (datetime('now')),
                    meta TEXT DEFAULT '{}'
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS organisms (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    molecule_ids_json TEXT,
                    template TEXT,
                    first_seen TEXT DEFAULT (datetime('now')),
                    meta TEXT DEFAULT '{}'
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS conventions (
                    id TEXT PRIMARY KEY,
                    pattern TEXT NOT NULL,
                    description TEXT,
                    confidence REAL DEFAULT 0.0,
                    occurrences INTEGER DEFAULT 0,
                    scope TEXT,
                    first_seen TEXT DEFAULT (datetime('now')),
                    meta TEXT DEFAULT '{}'
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY,
                    provider TEXT,
                    model TEXT,
                    summary TEXT,
                    significance REAL DEFAULT 0.0,
                    tags_json TEXT DEFAULT '[]',
                    created_at TEXT DEFAULT (datetime('now')),
                    processed_at TEXT,
                    meta TEXT DEFAULT '{}'
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS decisions (
                    id TEXT PRIMARY KEY,
                    session_id TEXT REFERENCES sessions(id),
                    decision TEXT NOT NULL,
                    rationale TEXT,
                    project TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    meta TEXT DEFAULT '{}'
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    entity_type TEXT,
                    attributes_json TEXT DEFAULT '{}',
                    first_seen TEXT DEFAULT (datetime('now')),
                    last_seen TEXT DEFAULT (datetime('now')),
                    meta TEXT DEFAULT '{}'
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS anomalies (
                    id TEXT PRIMARY KEY,
                    type TEXT,
                    description TEXT NOT NULL,
                    evidence TEXT,
                    severity TEXT DEFAULT 'MEDIUM',
                    state TEXT DEFAULT 'active',
                    session_id TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    resolved_at TEXT,
                    meta TEXT DEFAULT '{}'
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS nudges (
                    id TEXT PRIMARY KEY,
                    description TEXT NOT NULL,
                    category TEXT,
                    priority TEXT DEFAULT 'MEDIUM',
                    state TEXT DEFAULT 'active',
                    session_id TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    resolved_at TEXT,
                    meta TEXT DEFAULT '{}'
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS project_state (
                    project TEXT PRIMARY KEY,
                    status TEXT,
                    one_liner TEXT,
                    updated_at TEXT DEFAULT (datetime('now')),
                    meta TEXT DEFAULT '{}'
                )
            """)
            
            # === COMPRESSION LOG (flexible layers + algorithm versioning) ===
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS compression_log (
                    id TEXT PRIMARY KEY,
                    timestamp TEXT DEFAULT (datetime('now')),
                    provider TEXT,
                    model TEXT,
                    conversation_id TEXT,
                    session_id TEXT,
                    tokens_original_in INTEGER,
                    tokens_compressed_in INTEGER,
                    tokens_original_out INTEGER,
                    tokens_compressed_out INTEGER,
                    compression_ratio_in REAL,
                    compression_ratio_out REAL,
                    layers TEXT DEFAULT '[]',
                    pattern_ref_hits INTEGER DEFAULT 0,
                    dictionary_version TEXT,
                    tokenizer TEXT,
                    meta TEXT DEFAULT '{}'
                )
            """)
            
            # === DICTIONARY VERSIONING (append-only, immutable symbols) ===
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dictionary_versions (
                    version TEXT PRIMARY KEY,
                    created_at TEXT DEFAULT (datetime('now')),
                    entries_count INTEGER,
                    dictionary TEXT NOT NULL,
                    delta_from TEXT,
                    delta TEXT
                )
            """)
            
            # === PROCESSING QUEUE (persistent) ===
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS queue (
                    id TEXT PRIMARY KEY,
                    intake_type TEXT NOT NULL,
                    content_type TEXT,
                    payload TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    priority INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT (datetime('now')),
                    started_at TEXT,
                    completed_at TEXT,
                    error TEXT,
                    attempts INTEGER DEFAULT 0,
                    meta TEXT DEFAULT '{}'
                )
            """)
            
            # === IDEMPOTENT INTAKE ===
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS intake_hashes (
                    content_hash TEXT PRIMARY KEY,
                    intake_type TEXT,
                    received_at TEXT DEFAULT (datetime('now')),
                    queue_id TEXT
                )
            """)
            
            # === EPIGENETIC INFRASTRUCTURE ===
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS meta_namespaces (
                    namespace TEXT PRIMARY KEY,
                    registered_by TEXT NOT NULL,
                    registered_at TEXT DEFAULT (datetime('now')),
                    fields_schema TEXT,
                    description TEXT,
                    applies_to TEXT DEFAULT '["atoms"]',
                    version TEXT DEFAULT '1.0'
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS type_registry (
                    type_name TEXT PRIMARY KEY,
                    category TEXT NOT NULL,
                    handler TEXT,
                    registered_by TEXT NOT NULL,
                    registered_at TEXT DEFAULT (datetime('now')),
                    config TEXT DEFAULT '{}',
                    active INTEGER DEFAULT 1
                )
            """)
            
            # === EVENT LOG (audit trail) ===
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS meta_events (
                    id TEXT PRIMARY KEY,
                    target_table TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    action TEXT NOT NULL,
                    old_value TEXT,
                    new_value TEXT,
                    written_by TEXT,
                    timestamp TEXT DEFAULT (datetime('now'))
                )
            """)
            
            # === LANGUAGE HEURISTICS (Tier 3 self-generated scanner rules) ===
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS language_heuristics (
                    language TEXT PRIMARY KEY,
                    rules_json TEXT NOT NULL,
                    generated_from INTEGER NOT NULL,
                    generated_at TEXT DEFAULT (datetime('now')),
                    generated_by TEXT,
                    accuracy_score REAL,
                    meta TEXT DEFAULT '{}'
                )
            """)
            
            # === INDEXES ===
            
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_atoms_structural_fp ON atoms(structural_fp, fp_version)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_atoms_semantic_fp ON atoms(semantic_fp, fp_version)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_atoms_fp_version ON atoms(fp_version)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_atoms_name ON atoms(name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_molecules_name ON molecules(name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_sessions_created ON sessions(created_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_sessions_significance ON sessions(significance)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status, priority)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_anomalies_state ON anomalies(state)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_nudges_state ON nudges(state)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_meta_events_target ON meta_events(target_table, target_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_meta_events_namespace ON meta_events(namespace)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_compression_log_session ON compression_log(session_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_compression_log_dict_ver ON compression_log(dictionary_version)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_type_registry_category ON type_registry(category)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_intake_hashes_received ON intake_hashes(received_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_language_heuristics_lang ON language_heuristics(language)")
            
            conn.commit()
            logger.info("Epigenetic schema initialized successfully")
            
            # Bootstrap data
            self._bootstrap_registry(cursor)
            conn.commit()
            logger.info("Bootstrap data loaded")
    
    def _bootstrap_registry(self, cursor):
        """Bootstrap type_registry, meta_namespaces, and dictionary_versions"""
        
        # Content type registry
        content_types = [
            ('CODE', 'content_type', 'services.scanner.handle_code', 'cortex_v1'),
            ('ACTIONS', 'content_type', 'services.parser.handle_actions', 'cortex_v1'),
            ('TEXT', 'content_type', 'services.haiku.handle_text', 'cortex_v1'),
            ('CHANGES', 'content_type', 'services.parser.handle_changes', 'cortex_v1'),
        ]
        
        for type_name, category, handler, registered_by in content_types:
            cursor.execute("""
                INSERT OR IGNORE INTO type_registry (type_name, category, handler, registered_by)
                VALUES (?, ?, ?, ?)
            """, (type_name, category, handler, registered_by))
        
        # Intake type registry
        intake_types = [
            ('exchange', 'intake_type', 'routers.intake.handle_exchange', 'cortex_v1'),
            ('summary', 'intake_type', 'routers.intake.handle_summary', 'cortex_v1'),
            ('tool_use', 'intake_type', 'routers.intake.handle_tool_use', 'cortex_v1'),
            ('import', 'intake_type', 'routers.intake.handle_import', 'cortex_v1'),
            ('webhook', 'intake_type', 'routers.intake.handle_webhook', 'cortex_v1'),
        ]
        
        for type_name, category, handler, registered_by in intake_types:
            cursor.execute("""
                INSERT OR IGNORE INTO type_registry (type_name, category, handler, registered_by)
                VALUES (?, ?, ?, ?)
            """, (type_name, category, handler, registered_by))
        
        # Embedding model registry
        cursor.execute("""
            INSERT OR IGNORE INTO type_registry (type_name, category, handler, registered_by, config)
            VALUES ('bge-m3', 'embedding_model', 'services.chromadb.embed_bge_m3', 'cortex_v1',
                    '{"model": "BAAI/bge-m3", "dimensions": 1024, "active": true}')
        """)
        
        # === Scanner tier registry (self-expanding three-tier scanner) ===
        
        cursor.execute("""
            INSERT OR IGNORE INTO type_registry (type_name, category, handler, registered_by, config)
            VALUES ('tree-sitter', 'scanner_tier', 'services.scanner.tree_sitter_parse', 'cortex_v1',
                    '{"priority": 1, "grammar_registry": "https://tree-sitter.github.io/tree-sitter/", "auto_download": true}')
        """)
        
        cursor.execute("""
            INSERT OR IGNORE INTO type_registry (type_name, category, handler, registered_by, config)
            VALUES ('llm-structural', 'scanner_tier', 'services.scanner.llm_analyze', 'cortex_v1',
                    '{"priority": 2, "model": "claude-haiku-4-5-20251001", "cost_per_file": 0.001}')
        """)
        
        cursor.execute("""
            INSERT OR IGNORE INTO type_registry (type_name, category, handler, registered_by, config)
            VALUES ('learned-heuristic', 'scanner_tier', 'services.scanner.heuristic_parse', 'cortex_v1',
                    '{"priority": 3, "generation_threshold": 30, "promotion_check_interval": "weekly"}')
        """)
        
        # Initial meta namespaces
        namespaces = [
            ('structural', 'scanner_v1', 'AST-derived structural metadata: language, lines, complexity, template_format', '["atoms", "molecules"]'),
            ('domain', 'classifier_v1', 'Domain classification with confidence scores and auto-clustering', '["atoms", "molecules", "organisms"]'),
            ('semantic', 'scanner_v1', 'Semantic tags and similarity markers', '["atoms", "molecules"]'),
            ('scanner_tier', 'scanner_v2',
             'Tracks which scanner tier analyzed this record, resolution method, and parse confidence',
             '["atoms", "molecules"]'),
        ]
        
        for namespace, registered_by, description, applies_to in namespaces:
            cursor.execute("""
                INSERT OR IGNORE INTO meta_namespaces (namespace, registered_by, description, applies_to)
                VALUES (?, ?, ?, ?)
            """, (namespace, registered_by, description, applies_to))
        
        # Initial dictionary version
        cursor.execute("""
            INSERT OR IGNORE INTO dictionary_versions (version, entries_count, dictionary, delta_from, delta)
            VALUES ('v1', 0, '{}', NULL, NULL)
        """)


# Global database instance
db = Database()


def get_db():
    """Get database instance"""
    return db
