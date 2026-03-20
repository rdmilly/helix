"""Migrate Knowledge Graph from Memory to Helix.
Extracts entities, relationships, mentions from Memory's knowledge_graph.db
and POSTs to Helix /api/v1/knowledge/migrate in batches.
"""
import sqlite3, json, sys, os
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 1)
try:
    import httpx
except ImportError:
    print('ERROR: run inside helix-cortex container'); sys.exit(1)

MEM_KG = os.getenv('MEM_KG', '/tmp/knowledge_graph.db')
HELIX = 'http://127.0.0.1:9050'
MENTION_BATCH = 5000

def main():
    print('=== Knowledge Graph Migration ===')
    # Verify Helix
    r = httpx.get(f'{HELIX}/api/v1/knowledge/stats', timeout=10)
    pre = r.json()
    print(f'Pre-migration Helix: {json.dumps(pre)}')

    # Load Memory KG
    db = sqlite3.connect(MEM_KG, timeout=30)
    db.row_factory = sqlite3.Row

    # --- Entities ---
    entities = []
    eid_to_name = {}
    for row in db.execute('SELECT * FROM entities'):
        eid_to_name[row['id']] = row['name']
        attrs = {}
        try: attrs = json.loads(row['attributes'])
        except: pass
        entities.append({
            'name': row['name'],
            'entity_type': row['entity_type'],
            'description': row['description'] or '',
            'attributes': attrs,
            'first_seen': row['first_seen'],
            'last_seen': row['last_seen'],
            'mention_count': row['mention_count'] or 0,
        })
    print(f'Loaded {len(entities)} entities')

    # --- Relationships (resolve IDs to names) ---
    relationships = []
    for row in db.execute('SELECT * FROM relationships'):
        src = eid_to_name.get(row['source_id'])
        tgt = eid_to_name.get(row['target_id'])
        if not src or not tgt:
            continue
        relationships.append({
            'source_name': src,
            'target_name': tgt,
            'relation_type': row['relation_type'],
            'description': row['description'] or '',
            'session_id': row['session_id'] or '',
            'created_at': row['created_at'],
        })
    print(f'Loaded {len(relationships)} relationships')

    # --- Mentions (resolve IDs to names, batch) ---
    mentions = []
    for row in db.execute('SELECT * FROM mentions'):
        ename = eid_to_name.get(row['entity_id'])
        if not ename:
            continue
        mentions.append({
            'entity_name': ename,
            'session_id': row['session_id'] or '',
            'context': row['context'] or '',
            'mentioned_at': row['mentioned_at'],
        })
    db.close()
    print(f'Loaded {len(mentions)} mentions')

    client = httpx.Client(timeout=120)

    # Send entities + relationships first (small payload)
    print('Migrating entities + relationships...')
    resp = client.post(f'{HELIX}/api/v1/knowledge/migrate', json={
        'entities': entities,
        'relationships': relationships,
        'mentions': [],
    }, timeout=60)
    if resp.status_code == 200:
        r = resp.json()
        print(f'  OK: {r.get("entities")} entities, {r.get("relationships")} relationships')
    else:
        print(f'  ERROR: {resp.status_code} {resp.text[:200]}')

    # Send mentions in batches
    total_m = 0
    for i in range(0, len(mentions), MENTION_BATCH):
        batch = mentions[i:i+MENTION_BATCH]
        resp = client.post(f'{HELIX}/api/v1/knowledge/migrate', json={
            'entities': [],
            'relationships': [],
            'mentions': batch,
        }, timeout=120)
        if resp.status_code == 200:
            r = resp.json()
            total_m += r.get('mentions', 0)
            print(f'  Mentions batch {i//MENTION_BATCH + 1}: {r.get("mentions")} ok (total: {total_m})')
        else:
            print(f'  Mentions batch ERROR: {resp.status_code}')

    client.close()

    # Final stats
    post = httpx.get(f'{HELIX}/api/v1/knowledge/stats', timeout=10).json()
    print('=' * 50)
    print(f'DONE: entities {pre.get("entities")} -> {post.get("entities")}')
    print(f'DONE: relationships {pre.get("relationships")} -> {post.get("relationships")}')
    print(f'DONE: mentions {pre.get("mentions")} -> {post.get("mentions")}')
    print('=' * 50)

if __name__ == '__main__': main()
