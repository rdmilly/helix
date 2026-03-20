#!/usr/bin/env python3
"""
backfill_intelligence.py - Backfill 9-tag intelligence extraction for existing sessions.

Re-processes sessions stored in structured_archive (collection='sessions') that don't
yet have intelligence items. Writes extracted tags to:
  - structured_archive (intelligence collection)
  - decisions, anomalies, conventions, kg_relationships tables
  - ChromaDB intelligence collection

Usage:
  docker exec helix-cortex python3 /app/scripts/backfill_intelligence.py --limit 50 --offset 0
  docker exec helix-cortex python3 /app/scripts/backfill_intelligence.py --limit 50 --offset 50

Background:
  nohup docker exec helix-cortex python3 /app/scripts/backfill_intelligence.py --limit 200 > /tmp/backfill_intel.log 2>&1 &
"""

import asyncio
import json
import logging
import sqlite3
import sys
import uuid
import argparse
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, '/app')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('backfill_intelligence')

from services.database import get_db_path
from services.haiku import get_haiku_service


def get_sessions_to_process(db_path: str, limit: int, offset: int) -> list:
    """Get session IDs from structured_archive that don't have intelligence items yet."""
    conn = sqlite3.connect(db_path)
    try:
        # Get sessions from the archive
        rows = conn.execute(
            "SELECT DISTINCT session_id FROM structured_archive WHERE collection='sessions' "
            "AND session_id IS NOT NULL ORDER BY session_id LIMIT ? OFFSET ?",
            (limit, offset)
        ).fetchall()
        all_sessions = [r[0] for r in rows if r[0]]

        # Filter out sessions already processed
        already_done = set()
        for sid in all_sessions:
            exists = conn.execute(
                "SELECT 1 FROM structured_archive WHERE collection='intelligence' AND session_id=? LIMIT 1",
                (sid,)
            ).fetchone()
            if exists:
                already_done.add(sid)

        pending = [s for s in all_sessions if s not in already_done]
        log.info(f"Sessions: {len(all_sessions)} total, {len(already_done)} already processed, {len(pending)} pending")
        return pending
    finally:
        conn.close()


def get_session_text(db_path: str, session_id: str) -> str:
    """Get content for a session from structured_archive."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT content FROM structured_archive WHERE session_id=? ORDER BY id LIMIT 10",
            (session_id,)
        ).fetchall()
        return " ".join(r[0] for r in rows if r[0])[:8000]
    finally:
        conn.close()


def write_intelligence_items(db_path: str, session_id: str, items: list) -> int:
    """Write extracted intelligence items to all destination tables."""
    if not items:
        return 0

    by_tag = defaultdict(list)
    for item in items:
        tag = item.get('tag', '')
        if tag:
            by_tag[tag].append(item)

    now = datetime.now(timezone.utc).isoformat()
    written = 0

    conn = sqlite3.connect(db_path, timeout=30)
    try:
        # structured_archive intelligence collection
        for item in items[:25]:
            tag = item.get('tag', '')
            content = item.get('content', '')
            if not tag or not content:
                continue
            item_id = str(uuid.uuid4())[:12]
            conn.execute(
                "INSERT OR IGNORE INTO structured_archive "
                "(id, collection, content, metadata_json, session_id, timestamp, created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (item_id, 'intelligence', content,
                 json.dumps({'tag': tag, 'component': item.get('component'),
                             'context': item.get('context', ''),
                             'confidence': item.get('confidence', 0.5),
                             'backfilled': True}),
                 session_id, now, now)
            )
            try:
                row = conn.execute("SELECT rowid FROM structured_archive WHERE id=?", (item_id,)).fetchone()
                if row:
                    conn.execute(
                        "INSERT OR IGNORE INTO structured_fts(rowid,content,collection) VALUES(?,?,?)",
                        (row[0], f"[{tag}] {content}", 'intelligence')
                    )
            except Exception:
                pass
            written += 1

        # DECISION -> decisions
        for item in by_tag.get('DECISION', []):
            content = item.get('content', '')
            if not content:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO decisions "
                "(id, session_id, decision, rationale, project, created_at, meta) "
                "VALUES (?,?,?,?,?,?,?)",
                (str(uuid.uuid4())[:12], session_id, content, item.get('context', ''),
                 item.get('component') or 'backfill', now,
                 json.dumps({'confidence': item.get('confidence', 0.7), 'backfilled': True}))
            )

        # RISK -> anomalies
        for item in by_tag.get('RISK', []):
            content = item.get('content', '')
            if not content:
                continue
            confidence = float(item.get('confidence', 0.7))
            severity = 'high' if confidence >= 0.85 else 'medium' if confidence >= 0.7 else 'low'
            conn.execute(
                "INSERT OR IGNORE INTO anomalies "
                "(id, type, description, evidence, severity, state, session_id, created_at, meta) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (str(uuid.uuid4())[:12], 'risk', content, item.get('context', ''),
                 severity, 'open', session_id, now,
                 json.dumps({'backfilled': True, 'component': item.get('component')}))
            )

        # PATTERN -> conventions
        for item in by_tag.get('PATTERN', []):
            content = item.get('content', '')
            if not content:
                continue
            existing = conn.execute(
                "SELECT id, occurrences FROM conventions WHERE pattern=?", (content,)
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE conventions SET occurrences=? WHERE id=?",
                    (existing[1] + 1, existing[0])
                )
            else:
                conn.execute(
                    "INSERT OR IGNORE INTO conventions "
                    "(id, pattern, description, confidence, occurrences, scope, first_seen, meta) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (str(uuid.uuid4())[:12], content, item.get('context', ''),
                     float(item.get('confidence', 0.7)), 1,
                     item.get('component') or 'general', now,
                     json.dumps({'backfilled': True}))
                )

        # COUPLING + INVARIANT -> kg_relationships
        for tag in ('COUPLING', 'INVARIANT'):
            for item in by_tag.get(tag, []):
                content = item.get('content', '')
                if not content:
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO kg_relationships "
                    "(source_name, target_name, relation_type, description, created_at, session_id) "
                    "VALUES (?,?,?,?,?,?)",
                    (item.get('component') or 'unknown', content[:80], tag,
                     content, now, session_id)
                )

        conn.commit()
    finally:
        conn.close()

    return written


async def backfill_session(haiku, db_path: str, session_id: str) -> dict:
    """Process one session: get text, extract intelligence, write results."""
    text = get_session_text(db_path, session_id)
    if not text or len(text) < 100:
        return {'session_id': session_id, 'status': 'skipped', 'reason': 'too_short'}

    items = await haiku.extract_intelligence(text)
    if not items:
        return {'session_id': session_id, 'status': 'no_items'}

    written = write_intelligence_items(db_path, session_id, items)
    return {'session_id': session_id, 'status': 'ok', 'items': len(items), 'written': written}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=50)
    parser.add_argument('--offset', type=int, default=0)
    parser.add_argument('--rate', type=float, default=0.8,
                        help='Seconds between Haiku calls (default 0.8)')
    args = parser.parse_args()

    db_path = get_db_path()
    haiku = get_haiku_service()

    sessions = get_sessions_to_process(db_path, args.limit, args.offset)
    if not sessions:
        log.info('No sessions to process - all done or offset too high')
        return

    log.info(f'Processing {len(sessions)} sessions (offset={args.offset}, rate={args.rate}s)')

    ok = skipped = failed = total_items = 0
    for i, session_id in enumerate(sessions):
        try:
            result = await backfill_session(haiku, db_path, session_id)
            status = result.get('status')
            items = result.get('items', 0)
            total_items += items
            if status == 'ok':
                ok += 1
                log.info(f"[{i+1}/{len(sessions)}] {session_id[:24]}: {items} items")
            else:
                skipped += 1
                log.debug(f"[{i+1}/{len(sessions)}] {session_id[:24]}: {status}")
        except Exception as e:
            log.error(f"[{i+1}/{len(sessions)}] {session_id[:24]}: FAILED - {e}")
            failed += 1

        if i < len(sessions) - 1:
            await asyncio.sleep(args.rate)

    log.info(f"DONE: {ok} ok, {skipped} skipped, {failed} failed, {total_items} total items")

    # Final table counts
    conn = sqlite3.connect(db_path)
    for table in ('decisions', 'anomalies', 'conventions', 'kg_relationships'):
        count = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        log.info(f'  {table}: {count} rows')
    intel = conn.execute(
        'SELECT COUNT(*) FROM structured_archive WHERE collection="intelligence"'
    ).fetchone()[0]
    log.info(f'  intelligence archive: {intel} rows')
    conn.close()


if __name__ == '__main__':
    asyncio.run(main())
