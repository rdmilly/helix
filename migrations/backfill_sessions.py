#!/usr/bin/env python3
"""
backfill_sessions.py
Backfill summaries for all sessions that have none.

Strategy:
  millyext-transcript  -> synthetic summary from meta.custom fields (no LLM)
  sessions with exchanges -> Haiku via OpenRouter
  other sessions       -> synthetic from available meta

After summarizing, upsert into embeddings table.
"""
import json, os, time, sys
import httpx
import psycopg2, psycopg2.extras

DSN = os.getenv('POSTGRES_DSN',
    'host=helix-postgres user=helix password=934d69eb7ce6a90710643e93efe36fcc dbname=helix')
EMBEDDINGS_URL = os.getenv('EMBEDDINGS_URL', 'http://helix-embeddings:8000')
OPENROUTER_KEY = os.getenv('OPENROUTER_API_KEY',
    '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')
MODEL = 'anthropic/claude-haiku-4-5'
EMBED_MODEL = 'bge-large-en-v1.5'


def embed_texts(client, texts):
    resp = client.post('/embed', json={'texts': texts, 'normalize': True}, timeout=60)
    resp.raise_for_status()
    return resp.json()['embeddings']


def haiku_summarize(http, session_id, provider, content):
    """Call OpenRouter to summarize session content."""
    prompt = f"""Summarize this conversation session in 2-3 sentences. Focus on what was accomplished, key decisions, and important outcomes. Be concrete.

Provider: {provider}
Session ID: {session_id}

Content:
{content[:6000]}

Summary:"""
    try:
        resp = http.post(
            'https://openrouter.ai/api/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {OPENROUTER_KEY}',
                'Content-Type': 'application/json',
                'HTTP-Referer': 'https://helix.millyweb.com',
                'X-Title': 'Helix Cortex Backfill',
            },
            json={
                'model': MODEL,
                'max_tokens': 200,
                'messages': [{'role': 'user', 'content': prompt}],
            },
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json()['choices'][0]['message']['content'].strip()
        else:
            print(f'  OpenRouter {resp.status_code}: {resp.text[:100]}')
            return None
    except Exception as e:
        print(f'  OpenRouter error: {e}')
        return None


def synthetic_summary(session):
    """Build a summary from meta.custom fields (no LLM)."""
    meta = session['meta'] if isinstance(session['meta'], dict) else json.loads(session['meta'] or '{}')
    custom = meta.get('custom', {})
    name = custom.get('name', '') or session['id']
    provider = session['provider'] or 'unknown'
    model = session['model'] or ''
    sig = custom.get('significance', 0)
    char_count = custom.get('char_count', 0)
    has_code = custom.get('has_code', False)
    has_decision = custom.get('has_decision', False)
    has_failure = custom.get('has_failure', False)
    chunks = custom.get('chunks', 0)

    parts = [f"Session: {name}"]
    if model:
        parts.append(f"Model: {model}")
    details = []
    if has_code: details.append('contains code')
    if has_decision: details.append('includes decisions')
    if has_failure: details.append('has failures/errors')
    if char_count: details.append(f'{char_count:,} chars across {chunks} chunks')
    if details:
        parts.append('Conversation ' + ', '.join(details) + f'. Significance: {sig:.0f}/100.')
    return ' '.join(parts)


def run():
    pg = psycopg2.connect(DSN)
    cur = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    embed_client = httpx.Client(base_url=EMBEDDINGS_URL)
    http = httpx.Client(timeout=30)

    # Fetch all sessions without summaries
    cur.execute("""
        SELECT s.id, s.provider, s.model, s.significance, s.meta, s.created_at
        FROM sessions s
        WHERE s.meta->>'summary' IS NULL OR s.meta->>'summary' = ''
        ORDER BY s.created_at DESC
    """)
    sessions = cur.fetchall()
    print(f'Sessions to backfill: {len(sessions)}')

    # Fetch exchange content per session
    cur.execute("""
        SELECT session_id,
               string_agg(COALESCE(what_happened,'') || ' ' || COALESCE(decision,'') || ' ' || COALESCE(reason,''), ' ' ORDER BY created_at) AS combined
        FROM exchanges
        WHERE what_happened != '' OR decision != ''
        GROUP BY session_id
    """)
    exchange_map = {r['session_id']: r['combined'].strip() for r in cur.fetchall()}

    llm_count = synth_count = embed_count = skip_count = 0

    for i, sess in enumerate(sessions):
        sid = sess['id']
        provider = sess['provider'] or 'unknown'
        meta = sess['meta'] if isinstance(sess['meta'], dict) else json.loads(sess['meta'] or '{}')

        summary = None

        # Try LLM first if we have exchange content
        exchange_text = exchange_map.get(sid, '')
        if exchange_text and len(exchange_text) > 50:
            summary = haiku_summarize(http, sid, provider, exchange_text)
            if summary:
                llm_count += 1

        # Fall back to synthetic
        if not summary:
            summary = synthetic_summary(sess)
            synth_count += 1

        if not summary:
            skip_count += 1
            continue

        # Write summary into session meta
        meta['summary'] = summary
        cur.execute(
            "UPDATE sessions SET meta = %s WHERE id = %s",
            (json.dumps(meta), sid)
        )

        # Embed and upsert into embeddings table
        try:
            vecs = embed_texts(embed_client, [summary])
            embed_meta = {'provider': provider, 'type': 'session', 'session_id': sid}
            cur.execute("""
                INSERT INTO embeddings (id, source_type, source_id, content, embedding, model, metadata)
                VALUES (%s, 'sessions', %s, %s, %s::vector, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    content   = EXCLUDED.content,
                    embedding = EXCLUDED.embedding,
                    metadata  = EXCLUDED.metadata,
                    created_at = NOW()
            """, (
                f'ses_{sid[:40]}', sid, summary[:4000],
                str(vecs[0]), EMBED_MODEL, json.dumps(embed_meta)
            ))
            embed_count += 1
        except Exception as e:
            print(f'  embed error for {sid}: {e}')

        pg.commit()

        if (i + 1) % 25 == 0 or (i + 1) == len(sessions):
            print(f'  [{i+1}/{len(sessions)}] llm={llm_count} synth={synth_count} embedded={embed_count} skip={skip_count}', flush=True)

        # Throttle LLM calls
        if llm_count % 10 == 0 and llm_count > 0:
            time.sleep(0.5)

    print(f'\nDone. LLM summaries: {llm_count} | Synthetic: {synth_count} | Embedded: {embed_count} | Skipped: {skip_count}')

    # Final state
    cur.execute("SELECT COUNT(*) AS n FROM sessions WHERE meta->>'summary' IS NOT NULL AND meta->>'summary' != ''")
    print(f'Sessions with summary: {cur.fetchone()["n"]}/{len(sessions) + llm_count + synth_count}')
    cur.execute("SELECT source_type, COUNT(*) FROM embeddings GROUP BY source_type ORDER BY source_type")
    print('\nEmbeddings table:')
    for r in cur.fetchall(): print(f'  {r["source_type"]}: {r["count"]}')

    pg.close()
    embed_client.close()
    http.close()


if __name__ == '__main__':
    run()
