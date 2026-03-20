#!/usr/bin/env python3
"""
re_embed.py - Phase 4: Populate pgvector embeddings table from existing PG data.

Sources:
  exchanges    -> source_type='sessions'     (what_happened + decision)
  sessions     -> source_type='sessions'     (session summary if present)
  entities     -> source_type='entities'     (name + description)
  structured_archive -> source_type='intelligence'  (content by collection)
  atoms        -> source_type='atoms'        (name + semantic tags from meta)

Embeddings endpoint: http://helix-embeddings:8000/embed
Target table: embeddings (id, source_type, source_id, content, embedding, model, metadata)
"""
import json, os, sys, time
import httpx
import psycopg2
import psycopg2.extras

DSN = os.getenv('POSTGRES_DSN',
    'host=helix-postgres user=helix password=934d69eb7ce6a90710643e93efe36fcc dbname=helix')
EMBEDDINGS_URL = os.getenv('EMBEDDINGS_URL', 'http://helix-embeddings:8000')
MODEL = 'bge-large-en-v1.5'
BATCH = 32   # embed N texts per HTTP call


def embed_batch(client, texts):
    resp = client.post('/embed', json={'texts': texts, 'normalize': True}, timeout=60)
    resp.raise_for_status()
    return resp.json()['embeddings']


def upsert_embeddings(cur, rows):
    """rows = list of (id, source_type, source_id, content, embedding_list, metadata_dict)"""
    for (eid, stype, sid, content, vec, meta) in rows:
        cur.execute("""
            INSERT INTO embeddings (id, source_type, source_id, content, embedding, model, metadata)
            VALUES (%s, %s, %s, %s, %s::vector, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                content    = EXCLUDED.content,
                embedding  = EXCLUDED.embedding,
                metadata   = EXCLUDED.metadata,
                created_at = NOW()
        """, (eid, stype, sid, content[:8000], str(vec), MODEL, json.dumps(meta)))


def run():
    pg  = psycopg2.connect(DSN)
    cur = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    client = httpx.Client(base_url=EMBEDDINGS_URL)

    # Health check
    r = client.get('/health')
    print(f'Embeddings sidecar: {r.json()}')

    total_embedded = 0

    def process_source(label, items):
        """items = list of (id, source_type, source_id, text, metadata_dict)"""
        nonlocal total_embedded
        if not items:
            print(f'  {label}: 0 items, skipping')
            return
        batched = []
        for i in range(0, len(items), BATCH):
            chunk = items[i:i+BATCH]
            texts = [x[3] for x in chunk]
            try:
                vecs = embed_batch(client, texts)
                rows = [
                    (chunk[j][0], chunk[j][1], chunk[j][2], chunk[j][3], vecs[j], chunk[j][4])
                    for j in range(len(chunk))
                ]
                upsert_embeddings(cur, rows)
                pg.commit()
                total_embedded += len(rows)
                print(f'  {label}: {i+len(chunk)}/{len(items)} embedded', flush=True)
            except Exception as e:
                print(f'  {label} batch {i} error: {e}', flush=True)
                pg.rollback()

    # --- 1. EXCHANGES -> sessions collection ---
    print('\n[1/5] Exchanges -> sessions')
    cur.execute("""
        SELECT id, what_happened, decision, reason, project, session_id, created_at
        FROM exchanges
        WHERE skip = 0
          AND (what_happened != '' OR decision != '')
        ORDER BY created_at DESC
        LIMIT 5000
    """)
    rows = cur.fetchall()
    items = []
    for r in rows:
        text = ' '.join(filter(None, [r['what_happened'], r['decision'], r['reason']]))
        text = text.strip()[:4000]
        if not text: continue
        meta = {'project': r['project'], 'session_id': r['session_id'], 'type': 'exchange'}
        items.append((f'exc_{r["id"]}', 'sessions', r['id'], text, meta))
    process_source('exchanges', items)

    # --- 2. SESSIONS -> sessions collection ---
    print('\n[2/5] Sessions -> sessions')
    cur.execute("""
        SELECT id, provider, model, significance, meta, created_at
        FROM sessions
        WHERE meta IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 2000
    """)
    rows = cur.fetchall()
    items = []
    for r in rows:
        meta_obj = r['meta'] if isinstance(r['meta'], dict) else json.loads(r['meta'] or '{}')
        summary = meta_obj.get('summary', '') or meta_obj.get('title', '')
        if not summary: continue
        meta = {'provider': r['provider'], 'model': r['model'] or '', 'type': 'session'}
        items.append((f'ses_{r["id"][:40]}', 'sessions', r['id'], summary[:4000], meta))
    process_source('sessions', items)

    # --- 3. ENTITIES -> entities collection ---
    print('\n[3/5] Entities -> entities')
    cur.execute("""
        SELECT id, name, entity_type, description, meta
        FROM entities
        WHERE name IS NOT NULL AND name != ''
        ORDER BY mention_count DESC NULLS LAST
        LIMIT 5000
    """)
    rows = cur.fetchall()
    items = []
    for r in rows:
        text = f"{r['name']}: {r['description'] or ''}".strip()
        if not text or text == f"{r['name']}:": text = r['name']
        meta = {'entity_type': r['entity_type'] or 'unknown'}
        items.append((f'ent_{r["id"][:40]}', 'entities', r['id'], text[:2000], meta))
    process_source('entities', items)

    # --- 4. STRUCTURED_ARCHIVE -> intelligence collection ---
    print('\n[4/5] Structured archive -> intelligence')
    cur.execute("""
        SELECT id, collection, content, session_id, timestamp
        FROM structured_archive
        WHERE content IS NOT NULL AND content != ''
        ORDER BY timestamp DESC NULLS LAST
        LIMIT 5000
    """)
    rows = cur.fetchall()
    items = []
    for r in rows:
        text = f"[{r['collection']}] {r['content']}".strip()[:4000]
        meta = {'collection': r['collection'], 'session_id': r['session_id'] or ''}
        items.append((f'arc_{r["id"][:40]}', 'intelligence', r['id'], text, meta))
    process_source('structured_archive', items)

    # --- 5. ATOMS -> atoms collection ---
    print('\n[5/5] Atoms -> atoms')
    cur.execute("""
        SELECT id, name, full_name, meta
        FROM atoms
        WHERE name IS NOT NULL
        ORDER BY occurrence_count DESC NULLS LAST
        LIMIT 5000
    """)
    rows = cur.fetchall()
    items = []
    for r in rows:
        meta_obj = r['meta'] if isinstance(r['meta'], dict) else json.loads(r['meta'] or '{}')
        tags = meta_obj.get('semantic_tags', []) or []
        category = meta_obj.get('category', 'function')
        text = f"{r['full_name'] or r['name']}: {category}. Tags: {', '.join(tags)}".strip()
        meta = {'category': category, 'tags': tags}
        items.append((f'atm_{r["id"][:40]}', 'atoms', r['id'], text[:2000], meta))
    process_source('atoms', items)

    # Summary
    cur.execute('SELECT source_type, COUNT(*) FROM embeddings GROUP BY source_type ORDER BY source_type')
    print('\n=== Embeddings table after re-embed ===')
    for row in cur.fetchall():
        print(f'  {row["source_type"]}: {row["count"]}')
    print(f'Total embedded this run: {total_embedded}')

    pg.close()
    client.close()


if __name__ == '__main__':
    run()
