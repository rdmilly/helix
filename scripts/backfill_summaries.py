#!/usr/bin/env python3
"""
backfill_summaries.py  --  Summarize existing conversation RAG sessions via Haiku

Usage (run inside helix-cortex container):
  python3 /app/scripts/backfill_summaries.py [--limit N] [--dry-run] [--offset N]
"""
import asyncio
import argparse
import json
import logging
import sqlite3
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, '/app')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('backfill')


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=50)
    parser.add_argument('--offset', type=int, default=0)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    from services.haiku import HaikuService
    from services.database import get_db_path
    from services import conversation_store
    from services.chromadb import get_chromadb_service

    haiku = HaikuService()
    db_path = get_db_path()
    chroma = get_chromadb_service()

    # Get existing summarized session IDs to skip
    with sqlite3.connect(db_path) as conn:
        existing = {r[0] for r in conn.execute(
            "SELECT session_id FROM structured_archive WHERE collection='sessions'"
        ).fetchall() if r[0]}
    logger.info(f'Already summarized: {len(existing)} sessions')

    # Get session IDs from the sessions table (primary store)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, provider, created_at FROM sessions "
            "ORDER BY created_at DESC "
            "LIMIT ? OFFSET ?",
            (args.limit + len(existing), args.offset)
        ).fetchall()

    sessions_to_process = [
        (r[0], r[1] or "anthropic", r[2]) for r in rows if r[0] not in existing
    ][:args.limit]

    logger.info(f'Processing {len(sessions_to_process)} sessions')

    processed = skipped = errors = 0

    for session_id, source, timestamp in sessions_to_process:
        # Get exchange text for this session from exchanges table
        with sqlite3.connect(db_path) as fts:
            chunks = fts.execute(
                "SELECT what_happened FROM exchanges WHERE what_happened IS NOT NULL "
                "AND session_id=? ORDER BY id ASC LIMIT 30",
                (session_id,)
            ).fetchall()

        if not chunks:
            skipped += 1
            continue

        full_text = '\n\n'.join(c[0] for c in chunks if c[0]).strip()
        if len(full_text) < 80:
            skipped += 1
            continue

        logger.info(f'[{processed+1}] {session_id} ({len(chunks)} chunks, {len(full_text)} chars)...')

        try:
            # Build message array from transcript
            messages = []
            for line in full_text.split('\n\n')[:40]:
                if line.startswith('Human:'):
                    messages.append({'role': 'user', 'content': line[6:].strip()})
                elif line.startswith('Assistant:'):
                    messages.append({'role': 'assistant', 'content': line[10:].strip()})
            if not messages:
                messages = [{'role': 'user', 'content': full_text[:3000]}]

            summary = await haiku.summarize_session(messages)
            if not summary or summary == 'Summary unavailable':
                skipped += 1
                continue

            decisions = await haiku.extract_decisions(full_text[:4000])

            if args.dry_run:
                logger.info(f'  DRY RUN: {summary[:120]}')
                processed += 1
                continue

            now = datetime.now(timezone.utc).isoformat()
            ts = timestamp or now

            with sqlite3.connect(db_path) as conn:
                # Session summary
                conn.execute(
                    'INSERT OR REPLACE INTO structured_archive '
                    '(id, collection, content, metadata_json, session_id, timestamp, created_at) '
                    'VALUES (?,?,?,?,?,?,?)',
                    (f'bf-{session_id[:38]}', 'sessions', summary,
                     json.dumps({'source': source or 'backfill', 'backfilled': True, 'chunks': len(chunks)}),
                     session_id, ts, now)
                )
                # FTS
                try:
                    row = conn.execute('SELECT rowid FROM structured_archive WHERE id=?',
                                       (f'bf-{session_id[:38]}',)).fetchone()
                    if row:
                        conn.execute('INSERT OR IGNORE INTO structured_fts(rowid,content,collection) VALUES(?,?,?)',
                                     (row[0], summary, 'sessions'))
                except Exception:
                    pass
                # Decisions
                for dec in decisions[:5]:
                    dec_text = dec.get('decision', '')
                    if not dec_text:
                        continue
                    conn.execute(
                        'INSERT INTO structured_archive '
                        '(id, collection, content, metadata_json, session_id, timestamp, created_at) '
                        'VALUES (?,?,?,?,?,?,?)',
                        (str(uuid.uuid4())[:12], 'decisions', dec_text,
                         json.dumps({'type': dec.get('type', 'general'), 'backfilled': True}),
                         session_id, ts, now)
                    )
                conn.commit()

            # Index summary into ChromaDB sessions collection
            try:
                await chroma.add_document(
                    collection_base='sessions',
                    doc_id=f'bf-{session_id[:38]}',
                    text=summary,
                    metadata={'session_id': session_id, 'source': source or 'backfill'}
                )
            except Exception as ce:
                logger.warning(f'ChromaDB index failed: {ce}')

            logger.info(f'  ✓ {len(decisions)} decisions')
            processed += 1
            await asyncio.sleep(0.8)  # Rate limit

        except Exception as e:
            logger.error(f'Error: {e}', exc_info=True)
            errors += 1
            await asyncio.sleep(2)

    logger.info(f'\nDone: {processed} processed, {skipped} skipped, {errors} errors')


if __name__ == '__main__':
    asyncio.run(main())
