"""
Haiku Reconciler - Hourly intelligence extraction from conversation archive.

Reads unprocessed entries from structured_archive, runs Haiku to extract:
- Decisions (architecture, patterns, rules)
- Patterns (recurring approaches)
- Failures (dead ends, negative signals)

After extraction, auto-appends a journal entry to journal.md so
every session's decisions automatically appear in the ADR list and journal.
"""
import json
import logging
import asyncio
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("helix.reconciler")

JOURNAL_PATH = Path("/app/working-kb/journal.md")


async def run_reconciler() -> dict:
    """
    Main reconciler loop. Pulls unprocessed archive entries,
    extracts intelligence via Haiku, writes back to Postgres
    and appends to journal.md.
    """
    from services.haiku import get_haiku_service
    from services import pg_sync

    haiku = get_haiku_service()

    # Check circuit breaker
    if hasattr(haiku, 'circuit_breaker') and haiku.circuit_breaker.failures > 5:
        log.warning("Reconciler: Haiku circuit breaker open, stopping")
        return {"processed": 0, "skipped": 1, "errors": 0}

    conn = pg_sync.sqlite_conn()
    stats = {"processed": 0, "decisions": 0, "patterns": 0,
             "failures": 0, "errors": 0, "skipped": 0}

    try:
        # Get watermark
        watermark = _get_watermark(conn)
        log.info(f"Reconciler starting. Watermark: {watermark}")

        # Pull unprocessed entries since watermark
        rows = conn.execute(
            """
            SELECT id, content, session_id, timestamp, collection
            FROM structured_archive
            WHERE created_at > %s
              AND collection IN ('exchanges', 'sessions', 'turns')
            ORDER BY created_at ASC
            LIMIT 50
            """,
            (watermark,)
        ).fetchall()

        if not rows:
            log.info("Reconciler: no new entries to process")
            return stats

        log.info(f"Reconciler: processing {len(rows)} entries")

        latest_ts = None
        session_decisions = {}  # session_id -> list of decisions

        for entry_id, content, session_id, timestamp, collection in rows:
            try:
                if not content or len(content.strip()) < 20:
                    stats["skipped"] += 1
                    continue

                # Extract decisions via Haiku
                decisions = await haiku.extract_decisions(content[:3000])

                if decisions:
                    for d in decisions:
                        decision_text = d.get("decision", "") if isinstance(d, dict) else str(d)
                        if decision_text and len(decision_text) > 10:
                            _write_archive_entry(conn, "decisions", decision_text, session_id)
                            stats["decisions"] += 1
                            # Track for journal entry
                            if session_id not in session_decisions:
                                session_decisions[session_id] = []
                            session_decisions[session_id].append(decision_text)

                # Extract patterns and failures
                intelligence = await haiku.extract_intelligence(content[:3000])
                if intelligence:
                    for item in intelligence:
                        item_content = item.get("content", "") if isinstance(item, dict) else str(item)
                        item_type = item.get("type", "pattern") if isinstance(item, dict) else "pattern"
                        if not item_content:
                            continue
                        if "fail" in item_type.lower() or "error" in item_type.lower():
                            _write_archive_entry(conn, "failures", item_content, session_id)
                            stats["failures"] += 1
                        else:
                            _write_archive_entry(conn, "patterns", item_content, session_id)
                            stats["patterns"] += 1

                stats["processed"] += 1
                latest_ts = timestamp
                await asyncio.sleep(0.5)

            except Exception as e:
                log.error(f"Reconciler: error processing entry {entry_id}: {e}")
                stats["errors"] += 1
                continue

        # Advance watermark
        if latest_ts:
            _set_watermark(conn, latest_ts)
            log.info(f"Reconciler: watermark advanced to {latest_ts}")

        # Write journal.md entries for sessions with decisions
        if session_decisions and stats["decisions"] > 0:
            try:
                await _append_journal_entries(conn, session_decisions, haiku)
            except Exception as e:
                log.warning(f"Reconciler: journal append failed: {e}")

    finally:
        conn.close()

    log.info(f"Reconciler complete: {stats}")
    return stats


async def _append_journal_entries(conn, session_decisions: dict, haiku) -> None:
    """Append new journal entries to journal.md for sessions with decisions."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Check what sessions already have journal entries today
    existing = ""
    if JOURNAL_PATH.exists():
        existing = JOURNAL_PATH.read_text(encoding="utf-8")

    new_entries = []
    for session_id, decisions in session_decisions.items():
        if not decisions:
            continue

        # Skip if session already has a journal entry
        if session_id[:8] in existing:
            continue

        # Use Haiku to generate a title and summary from the decisions
        try:
            decisions_text = "\n".join(f"- {d}" for d in decisions[:8])
            summary_result = await haiku.summarize_session(
                f"Session {session_id}\nDecisions made:\n{decisions_text}"
            )
            if isinstance(summary_result, dict):
                title = summary_result.get("title", f"Session {session_id[:8]}")
                summary = summary_result.get("summary", "")
            elif isinstance(summary_result, str):
                title = summary_result.split("\n")[0][:120]
                summary = summary_result[:400]
            else:
                title = f"Session {session_id[:8]}"
                summary = ""
        except Exception:
            title = f"Session {session_id[:8]}"
            summary = ""

        # Format journal entry
        dec_lines = "\n".join(f"- {d}" for d in decisions[:8])
        entry = f"""\n## {today} \u2014 {title}

{summary}

### Decisions
{dec_lines}

### Final glance
Auto-extracted by Haiku reconciler from session {session_id[:8]}.

---
"""
        new_entries.append(entry)

    if new_entries:
        # Prepend after the journal header
        if existing and "---" in existing:
            # Insert after first ---
            insert_pos = existing.find("---\n") + 4
            new_content = existing[:insert_pos] + "".join(new_entries) + existing[insert_pos:]
        else:
            header = "# Working Journal\n_Auto-appended by Haiku reconciler._\n\n---\n\n"
            new_content = header + "".join(new_entries) + (existing or "")

        JOURNAL_PATH.write_text(new_content, encoding="utf-8")
        log.info(f"Reconciler: appended {len(new_entries)} journal entries to journal.md")


def _get_watermark(conn) -> str:
    """Get the last processed timestamp."""
    try:
        row = conn.execute(
            "SELECT value FROM project_state WHERE project = %s AND status = %s",
            ("reconciler", "watermark")
        ).fetchone()
        if row and row[0]:
            return row[0]
    except Exception:
        pass
    return "2000-01-01T00:00:00+00:00"


def _set_watermark(conn, ts) -> None:
    """Advance the reconciler watermark."""
    try:
        conn.execute(
            """
            INSERT INTO project_state (project, status, one_liner, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (project) DO UPDATE
            SET status = EXCLUDED.status, one_liner = EXCLUDED.one_liner, updated_at = NOW()
            """,
            ("reconciler", "watermark", str(ts))
        )
        conn.commit()
    except Exception as e:
        log.debug(f"watermark set failed: {e}")


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
