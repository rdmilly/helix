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
                   WHERE sa.search_vector @@ plainto_tsquery('english', ?) AND sa.collection = ?
                   ORDER BY sa.timestamp DESC LIMIT ?''',
                (q, collection, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                '''SELECT sa.* FROM structured_archive sa
                   WHERE sa.search_vector @@ plainto_tsquery('english', ?)
                   ORDER BY sa.timestamp DESC LIMIT ?''',
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
            '''SELECT sa.*, rank FROM structured_archive sa
               JOIN structured_fts ON sa.rowid = structured_fts.rowid
               WHERE structured_fts MATCH ? AND sa.collection = 'decisions'
               ORDER BY rank LIMIT ?''',
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
            '''SELECT sa.*, rank FROM structured_archive sa
               JOIN structured_fts ON sa.rowid = structured_fts.rowid
               WHERE structured_fts MATCH ? AND sa.collection = 'failures'
               ORDER BY rank LIMIT ?''',
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
            '''SELECT sa.*, rank FROM structured_archive sa
               JOIN structured_fts ON sa.rowid = structured_fts.rowid
               WHERE structured_fts MATCH ? AND sa.collection = 'patterns'
               ORDER BY rank LIMIT ?''',
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
            '''SELECT sa.*, rank FROM structured_archive sa
               JOIN structured_fts ON sa.rowid = structured_fts.rowid
               WHERE structured_fts MATCH ? AND sa.collection = 'sessions'
               ORDER BY rank LIMIT ?''',
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
                'SELECT * FROM structured_archive WHERE collection = ? ORDER BY timestamp DESC LIMIT ?',
                (collection, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                'SELECT * FROM structured_archive ORDER BY timestamp DESC LIMIT ?',
                (limit,)
            ).fetchall()
        return {"collection": collection, "count": len(rows), "results": [_row_to_dict(r) for r in rows]}
    finally:
        conn.close()
