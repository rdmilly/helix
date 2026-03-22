"""Structured Archive Router — Query decisions, failures, patterns, sessions.

Searches the structured_archive table (migrated from Memory ChromaDB collections)
with FTS5 full-text search and metadata filtering.

Endpoints:
  GET  /api/v1/archive/search    - Search across all collections
  GET  /api/v1/archive/decisions  - Search decisions only
  GET  /api/v1/archive/failures   - Search failures only  
  GET  /api/v1/archive/patterns   - Search patterns only
  GET  /api/v1/archive/stats      - Collection statistics
"""
import json
import sqlite3
from services import pg_sync
import logging
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Query
from pydantic import BaseModel
from services.database import get_db_path

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/archive")

def _get_conn():
    conn = pg_sync.sqlite_conn(str(get_db_path()), timeout=10)
    return conn

def _row_to_dict(row) -> Dict:
    d = dict(row)
    if 'metadata_json' in d:
        try:
            d['metadata'] = pg_sync.dejson(d['metadata_json'])
        except:
            d['metadata'] = {}
        del d['metadata_json']
    return d

class ArchiveRecord(BaseModel):
    collection: str
    content: str
    session_id: str = "claude"
    metadata: dict = {}

@router.post("/record")
async def record_entry(data: ArchiveRecord):
    """Write a new entry to the structured archive."""
    import uuid
    from datetime import datetime, timezone
    valid = {"decisions", "failures", "patterns", "sessions", "project_archive", "snapshots"}
    if data.collection not in valid:
        from fastapi import HTTPException
        raise HTTPException(400, f"Invalid collection. Must be one of: {', '.join(sorted(valid))}")
    conn = _get_conn()
    try:
        entry_id = uuid.uuid4().hex[:12]
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """INSERT INTO structured_archive (id, collection, content, metadata_json, session_id, timestamp, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (entry_id, data.collection, data.content, json.dumps(data.metadata), data.session_id, now, now)
        )
        conn.commit()
        try:
            from services.event_bus import publish
            publish("archive.recorded", {"collection": data.collection, "entry_id": entry_id})
        except Exception:
            pass
        return {"status": "recorded", "id": entry_id, "collection": data.collection, "size": len(data.content)}
    finally:
        conn.close()

@router.get("/stats")
async def archive_stats():
    conn = _get_conn()
    try:
        total = conn.execute('SELECT COUNT(*) FROM structured_archive').fetchone()[0]
        by_col = {}
        for row in conn.execute('SELECT collection, COUNT(*) as cnt FROM structured_archive GROUP BY collection ORDER BY cnt DESC'):
            by_col[row['collection']] = row['cnt']
        return {"total": total, "by_collection": by_col}
    finally:
        conn.close()

@router.get("/search")
async def search_archive(
    q: str = Query(..., description="Search query"),
    collection: Optional[str] = Query(None, description="Filter by collection: decisions, failures, patterns, sessions, project_archive, snapshots, entities"),
    limit: int = Query(20, ge=1, le=100),
):
    conn = _get_conn()
    try:
        if collection:
            rows = conn.execute(
                '''SELECT sa.* FROM structured_archive sa
                   WHERE sa.search_vector @@ plainto_tsquery('english', %s) AND sa.collection = %s
                   ORDER BY sa.timestamp DESC LIMIT %s''',
                (q, collection, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                '''SELECT sa.* FROM structured_archive sa
                   WHERE sa.search_vector @@ plainto_tsquery('english', %s)
                   ORDER BY sa.timestamp DESC LIMIT %s''',
                (q, limit)
            ).fetchall()
        results = [_row_to_dict(r) for r in rows]
        return {"query": q, "collection": collection, "count": len(results), "results": results}
    finally:
        conn.close()

@router.get("/decisions")
async def search_decisions(q: str = Query(...), limit: int = Query(20, ge=1, le=100)):
    conn = _get_conn()
    try:
        rows = conn.execute(
            '''SELECT * FROM structured_archive
               WHERE search_vector @@ plainto_tsquery('english', %s) AND collection = 'decisions'
               ORDER BY created_at DESC LIMIT %s''',
            (q, limit)
        ).fetchall()
        return {"query": q, "count": len(rows), "results": [_row_to_dict(r) for r in rows]}
    finally:
        conn.close()

@router.get("/failures")
async def search_failures(q: str = Query(...), limit: int = Query(20, ge=1, le=100)):
    conn = _get_conn()
    try:
        rows = conn.execute(
            '''SELECT * FROM structured_archive
               WHERE search_vector @@ plainto_tsquery('english', %s) AND collection = 'failures'
               ORDER BY created_at DESC LIMIT %s''',
            (q, limit)
        ).fetchall()
        return {"query": q, "count": len(rows), "results": [_row_to_dict(r) for r in rows]}
    finally:
        conn.close()

@router.get("/patterns")
async def search_patterns(q: str = Query(...), limit: int = Query(20, ge=1, le=100)):
    conn = _get_conn()
    try:
        rows = conn.execute(
            '''SELECT * FROM structured_archive
               WHERE search_vector @@ plainto_tsquery('english', %s) AND collection = 'patterns'
               ORDER BY created_at DESC LIMIT %s''',
            (q, limit)
        ).fetchall()
        return {"query": q, "count": len(rows), "results": [_row_to_dict(r) for r in rows]}
    finally:
        conn.close()

@router.get("/sessions")
async def search_sessions(q: str = Query(...), limit: int = Query(20, ge=1, le=100)):
    conn = _get_conn()
    try:
        rows = conn.execute(
            '''SELECT * FROM structured_archive
               WHERE search_vector @@ plainto_tsquery('english', %s) AND collection = 'sessions'
               ORDER BY created_at DESC LIMIT %s''',
            (q, limit)
        ).fetchall()
        return {"query": q, "count": len(rows), "results": [_row_to_dict(r) for r in rows]}
    finally:
        conn.close()

@router.get("/recent")
async def recent_entries(
    collection: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    conn = _get_conn()
    try:
        if collection:
            rows = conn.execute(
                'SELECT * FROM structured_archive WHERE collection = %s ORDER BY timestamp DESC LIMIT %s',
                (collection, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                'SELECT * FROM structured_archive ORDER BY timestamp DESC LIMIT %s',
                (limit,)
            ).fetchall()
        return {"collection": collection, "count": len(rows), "results": [_row_to_dict(r) for r in rows]}
    finally:
        conn.close()
