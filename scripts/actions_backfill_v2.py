#!/usr/bin/env python3
"""
actions_backfill_v2.py — Mixed-tool sequence backfill.
Groups untagged tool calls into time-window sessions,
extracts cross-tool sequences, promotes to atom store.
"""
import sqlite3, json, hashlib
from datetime import datetime, timezone
from collections import defaultdict

DB = '/app/data/cortex.db'
WINDOW_MIN = 45
SEQ_WINDOW = 4
MIN_LEN = 2
THRESHOLD = 3

conn = sqlite3.connect(DB)

rows = conn.execute('''
    SELECT tool_name, server_name, timestamp
    FROM observer_actions
    WHERE (session_id IS NULL OR session_id = "")
    ORDER BY timestamp
''').fetchall()

print(f'{len(rows)} total untagged calls')

def encode(tool, server):
    t = tool.split('__')[-1]
    s = (server or '').replace('gateway', 'gw')
    if s and s not in t:
        return f'{t}@{s}'
    return t

def parse_ts(s):
    return datetime.strptime(s[:19], '%Y-%m-%dT%H:%M:%S')

sessions = []
current = []
prev_ts = None

for tool, server, ts in rows:
    key = encode(tool, server)
    if prev_ts is None:
        current.append(key)
        prev_ts = ts
        continue
    try:
        gap = (parse_ts(ts) - parse_ts(prev_ts)).total_seconds() / 60
    except Exception:
        gap = 0
    if gap > WINDOW_MIN:
        if len(current) >= 2:
            sessions.append(current)
        current = [key]
    else:
        current.append(key)
    prev_ts = ts

if len(current) >= 2:
    sessions.append(current)

print(f'{len(sessions)} time-window sessions')
print('Longest:', sorted([len(s) for s in sessions], reverse=True)[:5])

seq_counts = defaultdict(int)
seq_data = {}

for tools in sessions:
    seen = set()
    for start in range(len(tools)):
        for length in range(MIN_LEN, min(SEQ_WINDOW+1, len(tools)-start+1)):
            seq = tuple(tools[start:start+length])
            # Must have at least 2 different base tools
            if len(set(t.split('@')[0] for t in seq)) < 2:
                continue
            h = hashlib.md5(json.dumps(list(seq)).encode()).hexdigest()[:12]
            if h not in seen:
                seq_counts[h] += 1
                seq_data[h] = list(seq)
                seen.add(h)

promotable = sorted(
    [(h, seq_data[h], seq_counts[h]) for h, c in seq_counts.items() if c >= THRESHOLD],
    key=lambda x: x[2], reverse=True
)[:50]

print(f'\nMixed-tool sequences >= {THRESHOLD}x: {len(promotable)}')
print()

promoted = 0
for h, seq, count in promotable:
    name = '__then__'.join(s[:18] for s in seq)
    full = ' -> '.join(seq)
    existing = conn.execute('SELECT id FROM atoms WHERE name = ? LIMIT 1', (name,)).fetchone()
    if existing:
        conn.execute(
            'UPDATE atoms SET occurrence_count = ?, last_seen = ? WHERE name = ?',
            (count, datetime.now(timezone.utc).isoformat(), name)
        )
        print(f'  [{count:3d}x] UPDATE: {full}')
        continue
    code = f'# ACTIONS sequence (mixed-tool backfill)\n# {full}\n# Seen {count}x\n'
    for i, t in enumerate(seq):
        code += f'# Step {i+1}: {t}\n'
    meta = json.dumps({
        'structural': {'sequence': seq, 'type': 'ACTIONS', 'length': len(seq)},
        'semantic': {'category': 'ACTIONS', 'frequency': count, 'source': 'mixed_backfill'}
    })
    atom_id = f'act_{h}'
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT OR IGNORE INTO atoms
        (id, name, full_name, code, template, parameters_json,
         structural_fp, semantic_fp, fp_version,
         first_seen, last_seen, occurrence_count, meta)
        VALUES (?,?,?,?,NULL,'[]',?,?,'v1',?,?,?,?)
    """, (atom_id, name, full, code, h, h+'s', now, now, count, meta))
    conn.commit()
    print(f'  [{count:3d}x] {full}')
    promoted += 1

total = conn.execute('SELECT COUNT(*) FROM atoms').fetchone()[0]
conn.close()
print(f'\nPromoted: {promoted} | Total atoms: {total}')
