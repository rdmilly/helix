#!/usr/bin/env python3
"""Fix the 4 rows that failed due to null IDs and NUL characters."""
import sys, json, uuid, subprocess
try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "-q"])
    import psycopg2
    from psycopg2.extras import Json

import sqlite3

PG_DSN = "host=helix-postgres user=helix password=934d69eb7ce6a90710643e93efe36fcc dbname=helix"

sq = sqlite3.connect('/app/data/cortex.db')
pg = psycopg2.connect(PG_DSN)
pg.autocommit = False
pg_cur = pg.cursor()
sq_cur = sq.cursor()

# ── Fix 1: meta_events — 1 null-id row ───────────────────────────────
print("Fix 1: meta_events null id rows")
sq_cur.execute("""
    SELECT target_table, target_id, namespace, action,
           old_value, new_value, written_by, timestamp
    FROM meta_events WHERE id IS NULL
""")
null_rows = sq_cur.fetchall()
print(f"  Found {len(null_rows)} rows with null id")
for row in null_rows:
    new_id = str(uuid.uuid4())
    pg_cur.execute("""
        INSERT INTO meta_events
            (id, target_table, target_id, namespace, action,
             old_value, new_value, written_by, timestamp)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, (new_id,) + row)
    print(f"  Inserted id={new_id}")
pg.commit()

# ── Fix 2: observer_actions — missing row(s) with NUL chars ─────────────
print("\nFix 2: observer_actions missing rows (NUL chars)")
pg_cur.execute("SELECT id::text FROM observer_actions")
pg_ids = {r[0] for r in pg_cur.fetchall()}

sq_cur.execute("""
    SELECT id, timestamp, session_id, sequence_num, tool_name,
           server_name, category, arguments_json, result_summary,
           has_file_content, file_path, file_size, duration_ms,
           error, created_at
    FROM observer_actions
""")
all_sq = sq_cur.fetchall()
missing = [r for r in all_sq if str(r[0]) not in pg_ids]
print(f"  Found {len(missing)} missing rows")

for row in missing:
    cleaned = list(row)
    # Strip NUL chars from all text fields
    for i, v in enumerate(cleaned):
        if isinstance(v, str):
            cleaned[i] = v.replace('\x00', '')
    # arguments_json (index 7) -> JSONB
    if cleaned[7]:
        try:
            cleaned[7] = Json(json.loads(cleaned[7]))
        except Exception:
            cleaned[7] = Json(None)
    else:
        cleaned[7] = Json(None)
    try:
        pg_cur.execute("""
            INSERT INTO observer_actions
                (id, timestamp, session_id, sequence_num, tool_name,
                 server_name, category, arguments_json, result_summary,
                 has_file_content, file_path, file_size, duration_ms,
                 error, created_at, user_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'system')
            ON CONFLICT DO NOTHING
        """, cleaned)
        pg.commit()
        print(f"  Inserted observer_actions id={row[0]}")
    except Exception as e:
        pg.rollback()
        print(f"  FAILED id={row[0]}: {e}")

# ── Final verification ──────────────────────────────────────────────────
print("\nFinal counts:")
for table in ['meta_events', 'observer_actions', 'snapshot_queue', 'snapshots']:
    sq_cur.execute(f"SELECT count(*) FROM {table}")
    sn = sq_cur.fetchone()[0]
    pg_cur.execute(f"SELECT count(*) FROM {table}")
    pn = pg_cur.fetchone()[0]
    if sn == pn:
        flag = "✓ MATCH"
    elif table in ('snapshot_queue', 'snapshots') and pn == 0:
        flag = "⚠ SKIPPED (transient, regenerated on startup)"
    else:
        flag = "MISMATCH"
    print(f"  {flag:45s} {table}: sqlite={sn} pg={pn}")

sq.close()
pg.close()
print("\nDone.")
