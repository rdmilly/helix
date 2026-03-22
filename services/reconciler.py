"""Haiku Reconciler Service

Runs periodically (every hour via scheduler) to:
1. Pull recent structured_archive entries not yet reconciled
2. Run them through Haiku for decision/pattern/failure extraction
3. Write extracted intelligence back to structured_archive
4. Keep a watermark to avoid reprocessing

This is what makes helixmaster.millyweb.com genuinely dynamic —
every session's decisions automatically appear in the ADR list and journal.
"""
import json
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional
from services import pg_sync

log = logging.getLogger("helix.reconciler")

WATERMARK_KEY = "reconciler_watermark"


def _get_watermark(conn) -> Optional[str]:
    """Get last processed timestamp from structured_archive _meta collection."""
    try:
        row = conn.execute(
            "SELECT content FROM structured_archive WHERE collection = %s AND session_id = %s ORDER BY created_at DESC LIMIT 1",
            ('_meta', WATERMARK_KEY)
        ).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _set_watermark(conn, ts: str):
    """Write watermark to structured_archive _meta collection."""
    import uuid
    try:
        entry_id = uuid.uuid4().hex[:12]
        conn.execute(
            """
            INSERT INTO structured_archive (id, collection, content, metadata_json, session_id, timestamp, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """,
            (entry_id, '_meta', ts, '{}', WATERMARK_KEY, ts)
        )
        conn.commit()
    except Exception as e:
        log.warning(f"set_watermark failed: {e}")
        try:
            conn.rollback()
        except Exception:
            pass


async def run_reconciler() -> dict:
    """
    Main reconciler loop. Pulls unprocessed archive entries,
    extracts intelligence via Haiku, writes back.
    """
    from services.haiku import get_haiku_service
    haiku = get_haiku_service()

    stats = {"processed": 0, "decisions": 0, "patterns": 0, "failures": 0, "errors": 0, "skipped": 0}

    conn = pg_sync.sqlite_conn()
    try:
        watermark = _get_watermark(conn)
        log.info(f"Reconciler starting. Watermark: {watermark}")

        # Pull recent sessions + unclassified entries since watermark
        if watermark:
            rows = conn.execute(
                """
                SELECT id, content, session_id, created_at, collection
                FROM structured_archive
                WHERE created_at > %s
                  AND collection IN ('sessions', 'patterns')
                  AND length(content) > 50
                ORDER BY created_at ASC
                LIMIT 20
                """,
                (watermark,)
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, content, session_id, created_at, collection
                FROM structured_archive
                WHERE collection IN ('sessions', 'patterns')
                  AND length(content) > 50
                ORDER BY created_at DESC
                LIMIT 10
                """
            ).fetchall()

        if not rows:
            log.info("Reconciler: no new entries to process")
            return stats

        log.info(f"Reconciler: processing {len(rows)} entries")
        latest_ts = None

        for row in rows:
            entry_id, content, session_id, created_at, collection = row
            latest_ts = str(created_at)

            try:
                # Skip very short content
                if not content or len(content.strip()) < 50:
                    stats["skipped"] += 1
                    continue

                # Check Haiku circuit breaker
                if not haiku.circuit_breaker.can_execute():
                    log.warning("Reconciler: Haiku circuit breaker open, stopping")
                    break

                # Extract decisions from content
                decisions = await haiku.extract_decisions(content)
                for d in (decisions or []):
                    decision_text = d.get("decision") or d.get("text") or str(d)
                    if not decision_text or len(decision_text) < 10:
                        continue
                    _write_archive_entry(conn, "decisions", decision_text, session_id)
                    stats["decisions"] += 1

                # Phase 3: wire KG chain for this batch of decisions
                if decisions:
                    from services.intelligence_chain import run_intelligence_chain
                    _loop = __import__("asyncio").get_event_loop()
                    chain_stats = await _loop.run_in_executor(None, lambda: run_intelligence_chain(
                        session_id=session_id, decisions=decisions, patterns=[],
                        failures=[], date_str=str(created_at)[:10] if created_at else None
                    ))
                    log.info(f"Chain stats: {chain_stats}")

                # Extract patterns/failures from intelligence
                intel = await haiku.extract_intelligence(content)
                for item in (intel or []):
                    item_type = item.get("type", "pattern")
                    item_content = item.get("content") or item.get("text") or str(item)
                    if not item_content or len(item_content) < 10:
                        continue
                    if item_type in ("failure", "error", "bug"):
                        _write_archive_entry(conn, "failures", item_content, session_id)
                        stats["failures"] += 1
                    else:
                        _write_archive_entry(conn, "patterns", item_content, session_id)
                        stats["patterns"] += 1

                stats["processed"] += 1
                # Small delay to be kind to Haiku API
                await asyncio.sleep(0.5)

            except Exception as e:
                log.error(f"Reconciler: error processing entry {entry_id}: {e}")
                stats["errors"] += 1
                continue

        # Advance watermark
        if latest_ts:
            _set_watermark(conn, latest_ts)
            log.info(f"Reconciler: watermark advanced to {latest_ts}")

    finally:
        conn.close()

    log.info(f"Reconciler complete: {stats}")
    return stats


def _write_archive_entry(conn, collection: str, content: str, session_id: str):
    """Write a new entry to structured_archive via pg_sync (Postgres)."""
    import uuid
    entry_id = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO structured_archive
            (id, collection, content, metadata_json, session_id, timestamp, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (id) DO NOTHING
        """,
        (entry_id, collection, content, json.dumps({"source": "reconciler"}), session_id, now)
    )
    conn.commit()
