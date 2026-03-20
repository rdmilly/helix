#!/usr/bin/env python3
"""Migration: Add diff/shard tables to Helix

Adds:
  - snapshots table: base snapshots for shard assembly
  - snapshot_queue table: objects flagged for snapshotting
  - Indexes on meta_events for diff chain queries (timestamp range)

Safe to run multiple times (CREATE TABLE IF NOT EXISTS).
"""
import sqlite3
import sys
from pathlib import Path

DB_PATH = "/opt/projects/helix/data/cortex.db"

MIGRATIONS = [
    # snapshots: base state for an object at a point in time
    """
    CREATE TABLE IF NOT EXISTS snapshots (
        id TEXT PRIMARY KEY,
        target_table TEXT NOT NULL,
        target_id TEXT NOT NULL,
        content TEXT NOT NULL,           -- JSON: full state + summary
        created_at TEXT DEFAULT (datetime('now'))
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_snapshots_target ON snapshots(target_table, target_id, created_at DESC);",

    # snapshot_queue: objects with too many diffs needing a new snapshot
    """
    CREATE TABLE IF NOT EXISTS snapshot_queue (
        id TEXT PRIMARY KEY,
        target_table TEXT NOT NULL,
        target_id TEXT NOT NULL,
        reason TEXT DEFAULT 'manual',
        queued_at TEXT DEFAULT (datetime('now')),
        processed_at TEXT
    );
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_queue_target ON snapshot_queue(target_table, target_id) WHERE processed_at IS NULL;",

    # Better index for diff chain queries (timestamp range on namespace)
    "CREATE INDEX IF NOT EXISTS idx_meta_events_diff ON meta_events(target_table, target_id, namespace, timestamp);",
]

def run_migration():
    print(f"Running diff/shard migration on {DB_PATH}")
    if not Path(DB_PATH).exists():
        print(f"ERROR: Database not found: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()

    for i, sql in enumerate(MIGRATIONS, 1):
        try:
            cursor.execute(sql)
            print(f"  [{i}/{len(MIGRATIONS)}] OK")
        except sqlite3.OperationalError as e:
            print(f"  [{i}/{len(MIGRATIONS)}] WARN: {e}")

    conn.commit()

    # Verify
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('snapshots','snapshot_queue')")
    tables = [r[0] for r in cursor.fetchall()]
    print(f"\nVerified tables: {tables}")

    cursor.execute("SELECT COUNT(*) FROM meta_events")
    print(f"meta_events rows: {cursor.fetchone()[0]}")

    conn.close()
    print("\nMigration complete.")

if __name__ == "__main__":
    run_migration()
