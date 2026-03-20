#!/usr/bin/env python3
"""
Helix Phase 3 — SQLite → PostgreSQL data migration
Run inside helix-cortex container:
  docker exec helix-cortex python3 /opt/projects/helix/migrations/migrate_sqlite_to_postgres.py
"""

import subprocess, sys

# Install psycopg2-binary if missing
try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("Installing psycopg2-binary...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "-q"])
    import psycopg2
    import psycopg2.extras

import sqlite3, json, shutil, os
from datetime import datetime, timezone
from pathlib import Path
from psycopg2.extras import Json

# ── Config ─────────────────────────────────────────────────────────────────
SQLITE_PATH = "/app/data/cortex.db"
BACKUP_DIR  = "/app/data/backups"
PG_DSN      = "host=helix-postgres user=helix password=934d69eb7ce6a90710643e93efe36fcc dbname=helix"
BATCH_SIZE  = 500

# ── Tables to skip ────────────────────────────────────────────────────────────
SKIP_TABLES = {
    "sqlite_sequence",
    "entity_fts", "entity_fts_config", "entity_fts_data", "entity_fts_docsize", "entity_fts_idx",
    "exchanges_fts", "exchanges_fts_config", "exchanges_fts_data", "exchanges_fts_docsize", "exchanges_fts_idx",
    "kb_fts", "kb_fts_config", "kb_fts_data", "kb_fts_docsize", "kb_fts_idx",
    "structured_fts", "structured_fts_config", "structured_fts_data", "structured_fts_docsize", "structured_fts_idx",
}

# ── JSONB columns per table ──────────────────────────────────────────────────
JSONB_COLS = {
    "sessions":            {"tags_json", "meta"},
    "exchanges":           {"files_changed", "services_changed", "entities_mentioned",
                           "relationships_found", "open_questions", "session_goals",
                           "actions_taken", "tools_used"},
    "entities":            {"attributes_json", "meta"},
    "anomalies":           {"meta"},
    "nudges":              {"meta"},
    "conventions":         {"meta"},
    "decisions":           {"meta"},
    "atoms":               {"parameters_json", "meta"},
    "molecules":           {"atom_ids_json", "atom_names_json", "meta"},
    "organisms":           {"molecule_ids_json", "meta"},
    "expressions":         {"parameter_map", "observed_from", "structural_params"},
    "compression_log":     {"layers", "meta"},
    "meta_namespaces":     {"fields_schema", "applies_to"},
    "project_state":       {"meta"},
    "runbook_pages":       {"source_config", "triggers"},
    "snapshots":           {"content"},
    "dictionary_versions": {"dictionary", "delta"},
    "type_registry":       {"config"},
    "membrain_events":     {"meta"},
    "membrain_users":      {"meta"},
    "observer_actions":    {"arguments_json"},
    "queue":               {"payload", "meta"},
    "structured_archive":  {"metadata_json"},
}

# ── Tables with BIGSERIAL PKs (need sequence reset) ───────────────────────────
BIGSERIAL_TABLES = {
    "kg_relationships", "kg_mentions", "membrain_events", "compression_profiles",
    "observer_actions", "observer_exchanges", "observer_facts", "observer_file_captures",
    "observer_sequences", "observer_session_tokens", "shard_diffs",
}


def safe_json(val):
    """
    Parse a SQLite TEXT value and wrap in psycopg2.extras.Json for JSONB insert.
    Returns Json(None) for null, Json(parsed) for valid JSON strings,
    Json({"_raw": val}) as fallback for unparseable strings.
    """
    if val is None:
        return Json(None)
    if isinstance(val, (dict, list)):
        return Json(val)
    if isinstance(val, str):
        try:
            return Json(json.loads(val))
        except (json.JSONDecodeError, ValueError):
            return Json({"_raw": val})
    # number, bool, etc.
    return Json(val)


def transform_row(table, columns, row):
    """Apply JSONB transforms to a row."""
    jsonb = JSONB_COLS.get(table, set())
    result = []
    for col, val in zip(columns, row):
        if col in jsonb:
            result.append(safe_json(val))
        else:
            result.append(val)
    return tuple(result)


def migrate_table(sq_conn, pg_conn, table, sq_columns):
    sq_cur = sq_conn.cursor()
    pg_cur = pg_conn.cursor()

    sq_cur.execute(f"SELECT count(*) FROM {table}")
    total = sq_cur.fetchone()[0]
    if total == 0:
        return 0

    col_names  = ", ".join(sq_columns)
    placeholders = ", ".join(["%s"] * len(sq_columns))
    insert_sql = (
        f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) "
        f"ON CONFLICT DO NOTHING"
    )

    offset   = 0
    inserted = 0

    while True:
        sq_cur.execute(f"SELECT {col_names} FROM {table} LIMIT {BATCH_SIZE} OFFSET {offset}")
        rows = sq_cur.fetchall()
        if not rows:
            break

        transformed = [transform_row(table, sq_columns, r) for r in rows]

        try:
            psycopg2.extras.execute_batch(pg_cur, insert_sql, transformed, page_size=BATCH_SIZE)
            pg_conn.commit()
            inserted += len(rows)
        except Exception as e:
            pg_conn.rollback()
            print(f"  BATCH ERROR in {table} @ offset {offset}: {e}")
            # Row-by-row fallback
            for i, t_row in enumerate(transformed):
                try:
                    pg_cur.execute(insert_sql, t_row)
                    pg_conn.commit()
                    inserted += 1
                except Exception as e2:
                    pg_conn.rollback()
                    print(f"    Row {offset+i} skipped: {e2}")

        offset += len(rows)
        if len(rows) < BATCH_SIZE:
            break

    return inserted


def reset_sequence(pg_conn, table):
    cur = pg_conn.cursor()
    cur.execute(f"""
        SELECT setval(
            pg_get_serial_sequence('{table}', 'id'),
            COALESCE((SELECT MAX(id) FROM {table}), 0) + 1,
            false
        )
    """)
    pg_conn.commit()


def verify_counts(sq_conn, pg_conn, tables):
    print("\n=== VERIFICATION ===")
    sq_cur = sq_conn.cursor()
    pg_cur = pg_conn.cursor()
    all_ok = True
    for table in sorted(tables):
        sq_cur.execute(f"SELECT count(*) FROM {table}")
        sq_n = sq_cur.fetchone()[0]
        pg_cur.execute(f"SELECT count(*) FROM {table}")
        pg_n = pg_cur.fetchone()[0]
        ok = sq_n == pg_n
        if not ok:
            all_ok = False
        if sq_n > 0 or pg_n > 0:
            flag = "✓" if ok else "MISMATCH ⚠️"
            print(f"  {flag:12s}  {table}: sqlite={sq_n}  pg={pg_n}")
    return all_ok


def main():
    # Backup
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    backup_path = f"{BACKUP_DIR}/cortex.db.pre-pg-{ts}"
    shutil.copy2(SQLITE_PATH, backup_path)
    print(f"Backup → {backup_path}")

    # Connect
    sq = sqlite3.connect(SQLITE_PATH)
    pg = psycopg2.connect(PG_DSN)
    pg.autocommit = False
    print(f"Connected. SQLite={SQLITE_PATH}")

    # Get tables
    sq_cur = sq.cursor()
    sq_cur.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' ORDER BY name
    """)
    all_tables = [r[0] for r in sq_cur.fetchall() if r[0] not in SKIP_TABLES]
    print(f"Migrating {len(all_tables)} tables...\n")

    migrated_tables = []
    total_rows = 0

    for table in all_tables:
        # SQLite columns
        sq_cur.execute(f"PRAGMA table_info({table})")
        sq_cols = [r[1] for r in sq_cur.fetchall()]

        # Postgres columns (exclude trigger-managed search_vector)
        pg_cur = pg.cursor()
        pg_cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'public'
        """, (table,))
        pg_col_set = {r[0] for r in pg_cur.fetchall()}

        cols = [c for c in sq_cols if c in pg_col_set and c != 'search_vector']

        if not cols:
            print(f"  {table}: no matching columns — skip")
            continue

        n = migrate_table(sq, pg, table, cols)
        total_rows += n
        migrated_tables.append(table)

        if n > 0:
            print(f"  ✓  {table}: {n} rows")
            if table in BIGSERIAL_TABLES:
                reset_sequence(pg, table)

    print(f"\nTotal rows inserted: {total_rows}")

    ok = verify_counts(sq, pg, migrated_tables)

    # FTS spot-check
    print("\n=== FTS SPOT-CHECK ===")
    pg_cur = pg.cursor()
    pg_cur.execute("SELECT name FROM entities WHERE search_vector @@ to_tsquery('english', 'helix') LIMIT 5")
    print(f"  entities @@ 'helix': {[r[0] for r in pg_cur.fetchall()]}")
    pg_cur.execute("SELECT count(*) FROM exchanges WHERE search_vector IS NOT NULL")
    print(f"  exchanges with search_vector: {pg_cur.fetchone()[0]}")

    sq.close()
    pg.close()

    if ok:
        print("\n✅ Phase 3 complete — all row counts match.")
        sys.exit(0)
    else:
        print("\n⚠️  Mismatches detected — review above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
