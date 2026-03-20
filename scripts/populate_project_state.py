#!/usr/bin/env python3
"""Populate project_state with per-component rows."""
import sqlite3, json, sys
sys.path.insert(0, '/app')
from services.database import get_db_path
conn = sqlite3.connect(get_db_path())

components = [
    ('ext-ingest', 'active', 'Receives MemBrain Chrome flushes, extracts 9-tag intelligence, routes to all DB tables and ChromaDB',
     {'file': 'routers/ext_ingest.py', 'version': '3', 'last_changed': '2026-03-11', 'key_deps': ['haiku', 'chromadb-service', 'structured_archive']}),
    ('synapse', 'active', 'Context assembly engine - searches atoms/sessions/intelligence, builds injection_text for MemBrain',
     {'file': 'services/synapse.py', 'last_changed': '2026-03-11', 'key_deps': ['chromadb-service', 'conversation_store', 'meta']}),
    ('haiku', 'active', 'Anthropic Haiku wrapper - extract_intelligence(), extract_entities(), summarize_session()',
     {'file': 'services/haiku.py', 'model': 'claude-haiku-4-5-20251001', 'last_changed': '2026-03-11'}),
    ('observer', 'active', 'Tool call logger - receives provisioner action logs, tracks sequences, extracts facts',
     {'file': 'routers/observer.py', 'actions': 2471, 'last_changed': '2026-03-11', 'sequences_tracking': True}),
    ('workbench', 'active', 'Unified write layer - pipeline: write->version->scan->index->KG->observe->snapshot',
     {'file': 'services/workbench.py', 'last_changed': '2026-03-11', 'key_deps': ['forge', 'observer', 'snapshots']}),
    ('chromadb-service', 'active', '5 ChromaDB collections: atoms, sessions, entities, conversations, intelligence - BGE-large embeddings',
     {'file': 'services/chromadb.py', 'port': 8000, 'collections': ['atoms','sessions','entities','conversations','intelligence'], 'embedding_model': 'bge-large-en-v1.5'}),
    ('mcp-tools', 'active', 'MCP tool definitions: helix_file_write, helix_file_read, helix_search, entity_upsert, archive_record',
     {'file': 'mcp_tools.py', 'tool_count': 20, 'last_changed': '2026-03-11'}),
    ('forge', 'active', 'Pattern catalog and versioning - workspace CRUD, MinIO versioning, atom scanning (989 atoms)',
     {'port': 9095, 'atoms': 989, 'molecules': 113, 'domain': 'forge.millyweb.com'}),
    ('provisioner', 'active', 'MCP gateway - 45 servers, 697 tools, routes all Claude tool calls, logs to observer',
     {'domain': 'mcp.millyweb.com', 'servers': 45, 'tools': 697}),
    ('membrain', 'active', 'Chrome extension v0.4.9 - captures turns, 2min flush to ext_ingest, context inject on send',
     {'version': '0.4.9', 'flush_interval': '2min', 'pending': 'Item 7: icons/options/CWS'}),
    ('snapshots', 'active', 'Component snapshot service - generates Haiku docs on file write, solves refamiliarization',
     {'file': 'services/snapshots.py', 'version': '1', 'last_changed': '2026-03-11'}),
    ('helix-cortex', 'active', 'Main FastAPI app port 9050 - hosts all routers, entry point for all Helix operations',
     {'port': 9050, 'domain': 'helix.millyweb.com', 'container': 'helix-cortex', 'version': '0.8.1'}),
]

conn.execute('DELETE FROM project_state')
for project, status, one_liner, meta in components:
    conn.execute(
        'INSERT OR REPLACE INTO project_state (project, status, one_liner, updated_at, meta) VALUES (?,?,?,?,?)',
        (project, status, one_liner, '2026-03-11', json.dumps(meta))
    )
conn.commit()

count = conn.execute('SELECT COUNT(*) FROM project_state').fetchone()[0]
rows = conn.execute('SELECT project, status FROM project_state').fetchall()
print(f'project_state: {count} rows')
for r in rows:
    print(f'  {r[0]}: {r[1]}')
conn.close()
