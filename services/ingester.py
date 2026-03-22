"""Conversation Ingester Service

Runs every 5min via scheduler. Pulls new chunks from conversations_fts.db
(SQLite, written by MemBrain extension) and syncs session records into
Postgres so search, KG extraction, and reconciler can work on them.

Flow: FTS SQLite -> deduplicate by session_id -> upsert Postgres sessions
      -> trigger Haiku enrichment on new sessions (async, non-blocking)
"""
import sqlite3
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from services import pg_sync

log = logging.getLogger("helix.ingester")

FTS_DB = Path("/app/data/conversations_fts.db")
WATERMARK_KEY = "ingester_watermark"


def _get_watermark(conn) -> str:
    try:
        row = conn.execute(
            "SELECT content FROM structured_archive WHERE collection = %s AND session_id = %s ORDER BY created_at DESC LIMIT 1",
            ('_meta', WATERMARK_KEY)
        ).fetchone()
        return row[0] if row else '2000-01-01T00:00:00+00:00'
    except Exception:
        return '2000-01-01T00:00:00+00:00'


def _set_watermark(conn, ts: str):
    import uuid
    entry_id = uuid.uuid4().hex[:12]
    try:
        conn.execute(
            "INSERT INTO structured_archive (id, collection, content, metadata_json, session_id, timestamp, created_at) VALUES (%s, %s, %s, %s, %s, %s, NOW())",
            (entry_id, '_meta', ts, '{}', WATERMARK_KEY, ts)
        )
        conn.commit()
    except Exception as e:
        log.warning(f"set_watermark: {e}")
        try: conn.rollback()
        except: pass


def run_ingester() -> dict:
    """Sync new FTS chunks into Postgres sessions. Returns stats dict."""
    if not FTS_DB.exists():
        return {"error": "FTS DB not found", "new_sessions": 0}

    stats = {"new_sessions": 0, "updated": 0, "skipped": 0, "errors": 0}

    pg = pg_sync.sqlite_conn()
    fts = sqlite3.connect(str(FTS_DB), timeout=10)
    fts.row_factory = sqlite3.Row

    try:
        watermark = _get_watermark(pg)
        log.info(f"Ingester watermark: {watermark}")

        # Pull chunks newer than watermark, grouped by session
        rows = fts.execute(
            """
            SELECT c1 as session_id,
                   MIN(c3) as first_ts,
                   MAX(c3) as last_ts,
                   COUNT(*) as chunk_count,
                   GROUP_CONCAT(c4, ' | ') as topics,
                   SUM(CASE WHEN json_extract(c6,'$.has_decision')='True' THEN 1 ELSE 0 END) as decisions,
                   SUM(CASE WHEN json_extract(c6,'$.has_code')='True' THEN 1 ELSE 0 END) as code_chunks,
                   json_extract(c6,'$.source') as source
            FROM conversation_fts_content
            WHERE c4 > ?
            GROUP BY c1
            ORDER BY last_ts ASC
            LIMIT 100
            """,
            (watermark,)
        ).fetchall()

        if not rows:
            log.info("Ingester: no new sessions")
            return stats

        log.info(f"Ingester: processing {len(rows)} sessions")
        latest_ts = watermark

        for row in rows:
            sid = row['session_id']
            try:
                # Check if already in Postgres
                existing = pg.execute(
                    "SELECT id, processed_at FROM sessions WHERE id = %s", (sid,)
                ).fetchone()

                tags = []
                if row['decisions'] > 0: tags.append('has_decisions')
                if row['code_chunks'] > 0: tags.append('has_code')
                meta = json.dumps({
                    'chunk_count': row['chunk_count'],
                    'decisions': row['decisions'],
                    'code_chunks': row['code_chunks'],
                    'source': row['source'],
                })

                if not existing:
                    pg.execute(
                        """
                        INSERT INTO sessions (id, provider, model, summary, significance, tags_json, created_at, meta)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        (sid, row['source'] or 'claude-ai', 'claude',
                         (row['topics'] or '')[:500],
                         min(1.0, (row['decisions'] * 0.2 + row['code_chunks'] * 0.1)),
                         json.dumps(tags),
                         row['first_ts'],
                         meta)
                    )
                    pg.commit()
                    stats['new_sessions'] += 1
                else:
                    stats['skipped'] += 1

                latest_ts = max(latest_ts, row['last_ts'] or watermark)
                stats['updated'] += 1  # count as processed regardless

            except Exception as e:
                log.error(f"Ingester: error on session {sid}: {e}")
                stats['errors'] += 1
                try: pg.rollback()
                except: pass

        # Always advance watermark to process next batch
        latest_ts = max(latest_ts, rows[-1]['last_ts'] if rows else watermark)
        if latest_ts != watermark:
            _set_watermark(pg, latest_ts)
            log.info(f"Ingester: watermark -> {latest_ts}")

    finally:
        fts.close()
        pg.close()

    log.info(f"Ingester complete: {stats}")
    return stats
