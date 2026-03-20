#!/usr/bin/env python3
import sqlite3, sys
sys.path.insert(0, '/app')
from services.database import get_db_path
c = sqlite3.connect(get_db_path())
print('=== INTELLIGENCE PIPELINE ===')
for t in ['decisions', 'anomalies', 'conventions', 'nudges', 'kg_relationships']:
    print('  ' + t + ':', c.execute('SELECT COUNT(*) FROM ' + t).fetchone()[0])
print('  intelligence items:', c.execute("SELECT COUNT(*) FROM structured_archive WHERE collection='intelligence'").fetchone()[0])
print('  session summaries:', c.execute("SELECT COUNT(*) FROM structured_archive WHERE collection='sessions'").fetchone()[0])
print('=== OBSERVER ===')
print('  actions:', c.execute('SELECT COUNT(*) FROM observer_actions').fetchone()[0])
print('  sequences:', c.execute('SELECT COUNT(*) FROM observer_sequences').fetchone()[0])
print('  facts:', c.execute('SELECT COUNT(*) FROM observer_facts').fetchone()[0])
print('=== COMPONENT STATE ===')
print('  project_state rows:', c.execute('SELECT COUNT(*) FROM project_state').fetchone()[0])
print('  snapshots:', c.execute('SELECT COUNT(*) FROM snapshots').fetchone()[0])
print('  snapshot_queue pending:', c.execute('SELECT COUNT(*) FROM snapshot_queue WHERE processed_at IS NULL').fetchone()[0])
rows = c.execute('SELECT target_id, created_at FROM snapshots ORDER BY created_at DESC').fetchall()
print('  snapshot list:', [(r[0], r[1][:10]) for r in rows])
c.close()
