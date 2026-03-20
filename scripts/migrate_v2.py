import sqlite3, json, time, sys, os
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 1)
try:
    import httpx
except ImportError:
    print('ERROR: run inside helix-cortex container'); sys.exit(1)

MEM_FTS = os.getenv('MEM_FTS', '/tmp/conversations_fts.db')
HELIX = 'http://127.0.0.1:9050'

def get_helix_sessions():
    r = httpx.get(f'{HELIX}/api/v1/conversations/stats', timeout=10)
    print(f'Helix stats: {r.json()}')
    p = '/app/data/conversations_fts.db'
    if os.path.exists(p):
        try:
            db = sqlite3.connect(p)
            cur = db.cursor()
            cur.execute('SELECT DISTINCT c1 FROM conversation_fts_content')
            s = set(row[0] for row in cur.fetchall())
            db.close()
            return s
        except Exception as e:
            print(f'FTS read error: {e}')
    return set()

def main():
    print('=== Migration v2 ===')
    existing = get_helix_sessions()
    print(f'Already in Helix: {len(existing)} sessions')
    db = sqlite3.connect(MEM_FTS, timeout=30)
    db.row_factory = sqlite3.Row
    cur = db.cursor()
    cur.execute("SELECT c0 as chunk_id, c1 as session_id, c2 as source, c3 as timestamp, c5 as content, c6 as meta_json FROM conversation_fts_content WHERE length(c5) > 50 AND c5 NOT LIKE '%Empty%' ORDER BY c3 ASC")
    rows = cur.fetchall()
    db.close()
    sessions = {}
    for r in rows:
        sid = r['session_id']
        if sid in existing: continue
        if sid not in sessions:
            sessions[sid] = {'chunks': [], 'source': r['source'], 'ts': r['timestamp']}
        sessions[sid]['chunks'].append({'id': r['chunk_id'], 'content': r['content'], 'meta': r['meta_json']})
        if r['timestamp'] and r['timestamp'] > sessions[sid]['ts']:
            sessions[sid]['ts'] = r['timestamp']
    print(f'Remaining: {len(sessions)} sessions')
    if not sessions: print('Nothing to do!'); return
    client = httpx.Client(timeout=180)
    ok = 0; errs = 0; ct = 0
    for i, (sid, data) in enumerate(sessions.items()):
        ordered = sorted(data['chunks'], key=lambda c: c['id'])
        text = '\n\n'.join(c['content'] for c in ordered)
        meta = {}
        try: meta = json.loads(ordered[0].get('meta', '{}'))
        except: pass
        try:
            resp = client.post(f'{HELIX}/api/v1/conversations/ingest', json={
                'text': text, 'session_id': sid,
                'source': data['source'] or 'migration',
                'timestamp': data['ts'] or '',
                'metadata': {'migrated': 'v2', 'name': meta.get('name','')},
                'scan_code': False,
            }, timeout=180)
            if resp.status_code == 200:
                ok += 1; ct += resp.json().get('chunks', 0)
            else:
                errs += 1
                if errs <= 10: print(f'  ERR {sid}: {resp.status_code}')
        except Exception as e:
            errs += 1
            if errs <= 10: print(f'  EXC {sid}: {type(e).__name__}')
        if (i+1) % 25 == 0:
            print(f'  {i+1}/{len(sessions)} ({ok} ok, {errs} err, {ct} chunks)')
            time.sleep(0.2)
    client.close()
    post = httpx.get(f'{HELIX}/api/v1/conversations/stats', timeout=10).json()
    print('=' * 50)
    print(f'DONE: {ok} migrated, {errs} errors, {ct} chunks')
    print(f'Helix: {post.get("fts_chunks")} chunks, {post.get("fts_sessions")} sessions')
    print('=' * 50)

if __name__ == '__main__': main()
