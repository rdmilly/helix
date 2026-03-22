"""KB Unification Router — Unified search across all knowledge bases.

Indexes Working KB and Infra KB markdown files into cortex.db
with FTS5 full-text search. Auto-reindexes on webhook triggers.

Endpoints:
  GET  /api/v1/kb/search     - Full-text search across all KB content
  GET  /api/v1/kb/stats      - Index statistics
  GET  /api/v1/kb/doc        - Get a specific document by path
  POST /api/v1/kb/reindex    - Trigger full reindex
  POST /api/v1/kb/index-file - Index a single file (for webhook updates)
"""
import json
import sqlite3
from services import pg_sync
import logging
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any

from fastapi import APIRouter, Query, BackgroundTasks
from pydantic import BaseModel
from services.database import get_db_path

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/kb")

# KB source directories (mounted into container)
KB_SOURCES = {
    "infra-kb": Path("/opt/projects/millyweb-kb"),
    "working-kb": Path("/app/working-kb"),
}

def _get_conn():
    conn = pg_sync.sqlite_conn(str(get_db_path()), timeout=10)
    return conn

def _init_kb_tables():
    """No-op: tables already exist in PostgreSQL."""
    pass
_init_kb_tables()

def _title_from_path(path: str) -> str:
    return Path(path).stem.replace('-', ' ').replace('_', ' ').title()

def _index_directory(source: str, base_path: Path) -> Dict[str, int]:
    if not base_path.exists():
        logger.warning(f"KB source path not found: {base_path}")
        return {"indexed": 0, "skipped": 0, "errors": 0}
    conn = _get_conn()
    indexed = skipped = errors = 0
    try:
        for md_file in sorted(base_path.rglob('*.md')):
            try:
                rel_path = str(md_file.relative_to(base_path))
                content = md_file.read_text(encoding='utf-8', errors='replace')
                if len(content.strip()) < 20:
                    skipped += 1
                    continue
                content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
                doc_id = f"{source}:{rel_path}"
                # Check if unchanged
                existing = conn.execute(
                    'SELECT content_hash FROM kb_documents WHERE id = %s', (doc_id,)
                ).fetchone()
                if existing and existing['content_hash'] == content_hash:
                    skipped += 1
                    continue
                # Extract title from first heading or filename
                title = _title_from_path(rel_path)
                for line in content.split('\n'):
                    if line.startswith('# '):
                        title = line[2:].strip()
                        break
                conn.execute(
                    '''INSERT INTO kb_documents
                       (id, source, path, title, content, content_hash, size_bytes, indexed_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (id) DO UPDATE SET
                         content=EXCLUDED.content,
                         content_hash=EXCLUDED.content_hash,
                         size_bytes=EXCLUDED.size_bytes,
                         indexed_at=EXCLUDED.indexed_at,
                         title=EXCLUDED.title''',
                    (doc_id, source, rel_path, title, content, content_hash,
                     len(content.encode('utf-8')),
                     datetime.now(timezone.utc).isoformat())
                )
                indexed += 1
            except Exception as e:
                errors += 1
                if errors <= 3:
                    logger.warning(f"KB index error {md_file}: {e}")
        conn.commit()
    finally:
        conn.close()
    return {"indexed": indexed, "skipped": skipped, "errors": errors}

@router.get("/stats")
async def kb_stats():
    conn = _get_conn()
    try:
        total = conn.execute('SELECT COUNT(*) FROM kb_documents').fetchone()[0]
        by_source = {}
        for row in conn.execute('SELECT source, COUNT(*) as cnt, SUM(size_bytes) as size FROM kb_documents GROUP BY source'):
            by_source[row['source']] = {"count": row['cnt'], "size_bytes": row['size']}
        return {"total_documents": total, "by_source": by_source}
    finally:
        conn.close()

@router.get("/search")
async def kb_search(
    q: str = Query(..., description="Search query"),
    source: Optional[str] = Query(None, description="Filter: infra-kb or working-kb"),
    limit: int = Query(20, ge=1, le=100),
):
    conn = _get_conn()
    try:
        if source:
            rows = conn.execute(
                '''SELECT * FROM kb_documents
                   WHERE search_vector @@ plainto_tsquery('english', %s) AND source = %s
                   ORDER BY indexed_at DESC LIMIT %s''',
                (q, source, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                '''SELECT * FROM kb_documents
                   WHERE search_vector @@ plainto_tsquery('english', %s)
                   ORDER BY indexed_at DESC LIMIT %s''',
                (q, limit)
            ).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            if len(d.get('content', '')) > 500:
                d['content_preview'] = d['content'][:500] + '...'
                d['content_length'] = len(d['content'])
                del d['content']
            results.append(d)
        return {"query": q, "source": source, "count": len(results), "results": results}
    finally:
        conn.close()

@router.get("/doc")
async def get_doc(
    path: str = Query(..., description="Document path (e.g. infrastructure/vps2-workloads.md)"),
    source: Optional[str] = Query(None),
):
    conn = _get_conn()
    try:
        if source:
            row = conn.execute('SELECT * FROM kb_documents WHERE path = %s AND source = %s', (path, source)).fetchone()
        else:
            row = conn.execute('SELECT * FROM kb_documents WHERE path = %s', (path,)).fetchone()
        if not row:
            return {"error": "not found", "path": path}
        return dict(row)
    finally:
        conn.close()

class IndexFileRequest(BaseModel):
    source: str
    path: str
    content: Optional[str] = None
    session_id: Optional[str] = None  # passed by file_events pipeline

@router.post("/index-file")
async def index_single_file(req: IndexFileRequest):
    """Index or re-index a single KB file. Called by file_events pipeline and webhooks."""
    conn = _get_conn()
    try:
        content = req.content
        if not content:
            base = KB_SOURCES.get(req.source)
            if base:
                fpath = base / req.path
                if fpath.exists():
                    content = fpath.read_text(encoding='utf-8', errors='replace')
        if not content or len(content.strip()) < 20:
            return {"status": "skipped", "reason": "empty or too short"}
        content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
        doc_id = f"{req.source}:{req.path}"
        title = _title_from_path(req.path)
        for line in content.split('\n'):
            if line.startswith('# '):
                title = line[2:].strip()
                break
        conn.execute(
            '''INSERT INTO kb_documents
               (id, source, path, title, content, content_hash, size_bytes, indexed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (id) DO UPDATE SET
                 content=EXCLUDED.content,
                 content_hash=EXCLUDED.content_hash,
                 size_bytes=EXCLUDED.size_bytes,
                 indexed_at=EXCLUDED.indexed_at,
                 title=EXCLUDED.title''',
            (doc_id, req.source, req.path, title, content, content_hash,
             len(content.encode('utf-8')),
             datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
        return {"status": "indexed", "id": doc_id, "title": title, "size": len(content)}
    finally:
        conn.close()

@router.post("/reindex")
async def reindex_all(background_tasks: BackgroundTasks):
    """Trigger full reindex of all KB sources."""
    def _do_reindex():
        results = {}
        for source, path in KB_SOURCES.items():
            results[source] = _index_directory(source, path)
            logger.info(f"KB reindex {source}: {results[source]}")
        conn = _get_conn()
        try:
            conn.execute("INSERT INTO kb_fts(kb_fts) VALUES('rebuild')")
            conn.commit()
        finally:
            conn.close()
    background_tasks.add_task(_do_reindex)
    return {"status": "reindex_started", "sources": list(KB_SOURCES.keys())}
