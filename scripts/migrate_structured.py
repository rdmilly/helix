"""Migrate Memory structured ChromaDB collections to Helix.
Ports: decisions, failures, sessions, project_archive, patterns, snapshots, entities.
Stores in cortex.db structured_archive table + Helix ChromaDB for semantic search.
"""
import sqlite3, json, sys, os, time
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 1)
try:
    import httpx
except ImportError:
    print('ERROR: run inside helix-cortex'); sys.exit(1)

CHROMADB = 'http://memory-chromadb:8000'
HELIX_DB = os.getenv('HELIX_DB', '/app/data/cortex.db')

COLLECTIONS = ['decisions', 'failures', 'sessions', 'project_archive', 'patterns', 'snapshots', 'entities']
BATCH = 100

def init_table(db):
    db.execute('''CREATE TABLE IF NOT EXISTS structured_archive (
        id TEXT PRIMARY KEY,
        collection TEXT NOT NULL,
        content TEXT NOT NULL,
        metadata_json TEXT DEFAULT '{}',
        session_id TEXT DEFAULT '',
        timestamp TEXT DEFAULT '',
        created_at TEXT DEFAULT (datetime('now'))
    )''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_sa_collection ON structured_archive(collection)')
    db.execute('CREATE INDEX IF NOT EXISTS idx_sa_session ON structured_archive(session_id)')
    db.execute('CREATE INDEX IF NOT EXISTS idx_sa_timestamp ON structured_archive(timestamp)')
    # FTS5 for text search
    db.execute('''CREATE VIRTUAL TABLE IF NOT EXISTS structured_fts USING fts5(
        content, collection, session_id,
        content='structured_archive',
        content_rowid='rowid'
    )''')
    # Triggers for FTS sync
    for op, prefix in [('INSERT', 'new'), ('DELETE', 'old')]:
        trigger_name = f'sa_fts_a{op[0].lower()}'
        try:
            if op == 'DELETE':
                db.execute(f'''CREATE TRIGGER IF NOT EXISTS {trigger_name} AFTER {op} ON structured_archive BEGIN
                    INSERT INTO structured_fts(structured_fts, rowid, content, collection, session_id)
                    VALUES('delete', old.rowid, old.content, old.collection, old.session_id);
                END''')
            else:
                db.execute(f'''CREATE TRIGGER IF NOT EXISTS {trigger_name} AFTER {op} ON structured_archive BEGIN
                    INSERT INTO structured_fts(rowid, content, collection, session_id)
                    VALUES({prefix}.rowid, {prefix}.content, {prefix}.collection, {prefix}.session_id);
                END''')
        except Exception as e:
            pass  # triggers may already exist
    db.commit()

def get_collection_id(name):
    r = httpx.get(f'{CHROMADB}/api/v1/collections', timeout=10)
    for c in r.json():
        if c['name'] == name:
            return c['id']
    return None

def fetch_all_docs(cid, total):
    docs = []
    offset = 0
    while offset < total:
        r = httpx.post(f'{CHROMADB}/api/v1/collections/{cid}/get', json={
            'limit': BATCH, 'offset': offset,
            'include': ['documents', 'metadatas']
        }, timeout=30)
        data = r.json()
        ids = data.get('ids', [])
        documents = data.get('documents', [])
        metadatas = data.get('metadatas', [])
        for i, doc_id in enumerate(ids):
            docs.append({
                'id': doc_id,
                'content': documents[i] if i < len(documents) else '',
                'metadata': metadatas[i] if i < len(metadatas) else {},
            })
        if len(ids) < BATCH:
            break
        offset += BATCH
    return docs

def main():
    print('=== Structured Collections Migration ===')
    db = sqlite3.connect(HELIX_DB, timeout=30)
    db.execute('PRAGMA journal_mode=WAL')
    init_table(db)
    
    before = db.execute('SELECT COUNT(*) FROM structured_archive').fetchone()[0]
    print(f'Existing archive entries: {before}')

    total_imported = 0
    for col_name in COLLECTIONS:
        cid = get_collection_id(col_name)
        if not cid:
            print(f'  {col_name}: NOT FOUND, skipping')
            continue
        count = httpx.get(f'{CHROMADB}/api/v1/collections/{cid}/count', timeout=10).json()
        if count == 0:
            print(f'  {col_name}: empty, skipping')
            continue

        print(f'  {col_name}: fetching {count} docs...')
        docs = fetch_all_docs(cid, count)
        
        ok = 0
        skipped = 0
        for doc in docs:
            content = doc['content'] or ''
            if not content or len(content) < 10:
                skipped += 1
                continue
            meta = doc['metadata'] or {}
            session_id = meta.get('session_id', '')
            timestamp = meta.get('timestamp', meta.get('created_at', meta.get('updated_at', '')))
            try:
                db.execute(
                    'INSERT OR REPLACE INTO structured_archive (id, collection, content, metadata_json, session_id, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                    (doc['id'], col_name, content, json.dumps(meta), session_id, timestamp)
                )
                ok += 1
            except Exception as e:
                if ok == 0: print(f'    ERR: {e}')
        db.commit()
        print(f'    -> {ok} imported, {skipped} skipped (empty)')
        total_imported += ok

    # Rebuild FTS
    print('Rebuilding FTS index...')
    db.execute("INSERT INTO structured_fts(structured_fts) VALUES('rebuild')")
    db.commit()

    after = db.execute('SELECT COUNT(*) FROM structured_archive').fetchone()[0]
    by_col = {}
    for row in db.execute('SELECT collection, COUNT(*) FROM structured_archive GROUP BY collection'):
        by_col[row[0]] = row[1]
    db.close()

    print('=' * 50)
    print(f'DONE: {total_imported} new entries imported')
    print(f'Total archive: {after} entries')
    print(f'By collection: {json.dumps(by_col, indent=2)}')
    print('=' * 50)

if __name__ == '__main__': main()
