#!/usr/bin/env python3
"""
backfill_intelligence.py
Embed the remaining 1,139 structured_archive rows not yet in pgvector.
Uses smaller batches (16) and shorter timeout to avoid the timeouts
that caused gaps in the original re_embed run.
"""
import json, os
import httpx
import psycopg2, psycopg2.extras

DSN = os.getenv('POSTGRES_DSN',
    'host=helix-postgres user=helix password=934d69eb7ce6a90710643e93efe36fcc dbname=helix')
EMBEDDINGS_URL = os.getenv('EMBEDDINGS_URL', 'http://helix-embeddings:8000')
MODEL = 'bge-large-en-v1.5'
BATCH = 16  # Smaller batch to avoid timeouts

def run():
    pg = psycopg2.connect(DSN)
    cur = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    client = httpx.Client(base_url=EMBEDDINGS_URL)

    cur.execute("""
        SELECT sa.id, sa.collection, sa.content, sa.session_id
        FROM structured_archive sa
        WHERE sa.content IS NOT NULL AND sa.content != ''
          AND NOT EXISTS (
              SELECT 1 FROM embeddings e WHERE e.id = 'arc_' || LEFT(sa.id, 40)
          )
        ORDER BY sa.timestamp DESC NULLS LAST
    """)
    rows = cur.fetchall()
    print(f'Missing intelligence embeddings: {len(rows)}')

    ok = err = 0
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i+BATCH]
        texts = [f"[{r['collection']}] {r['content']}".strip()[:4000] for r in chunk]
        try:
            resp = client.post('/embed', json={'texts': texts, 'normalize': True}, timeout=45)
            resp.raise_for_status()
            vecs = resp.json()['embeddings']
            for j, r in enumerate(chunk):
                meta = {'collection': r['collection'], 'session_id': r['session_id'] or ''}
                cur.execute("""
                    INSERT INTO embeddings (id, source_type, source_id, content, embedding, model, metadata)
                    VALUES (%s, 'intelligence', %s, %s, %s::vector, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    f'arc_{r["id"][:40]}', r['id'], texts[j][:8000],
                    str(vecs[j]), MODEL, json.dumps(meta)
                ))
            pg.commit()
            ok += len(chunk)
            print(f'  {ok}/{len(rows)}', flush=True)
        except Exception as e:
            print(f'  error at {i}: {e}', flush=True)
            pg.rollback()
            err += len(chunk)

    print(f'Done: {ok} embedded, {err} errors')
    cur.execute("SELECT COUNT(*) AS n FROM embeddings WHERE source_type='intelligence'")
    print(f'Total intelligence embeddings: {cur.fetchone()["n"]}')
    pg.close()
    client.close()

if __name__ == '__main__':
    run()
