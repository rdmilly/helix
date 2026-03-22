"""Master Status Router - Live data for helixmaster.millyweb.com

GET /api/v1/master/status - Full system snapshot for the helixmaster dashboard.
Called by fetchLiveData() on page load and refresh button.
"""
import time
import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from services import pg_sync

import socket
import http.client

log = logging.getLogger("helix.master_status")
router = APIRouter(prefix="/api/v1/master")

_START_TIME = time.time()
DATA_DIR = Path("/app/data")
FTS_DB = DATA_DIR / "conversations_fts.db"


def _uptime_str() -> str:
    elapsed = int(time.time() - _START_TIME)
    h, rem = divmod(elapsed, 3600)
    m, s = divmod(rem, 60)
    if h > 0: return f"{h}h {m}m"
    if m > 0: return f"{m}m {s}s"
    return f"{s}s"


def _chunk_count() -> int:
    try:
        if not FTS_DB.exists(): return 0
        conn = sqlite3.connect(str(FTS_DB), timeout=5)
        try:
            c = conn.execute("SELECT COUNT(*) FROM conversation_fts_content").fetchone()
            return c[0] if c else 0
        finally:
            conn.close()
    except Exception as e:
        log.warning(f"chunk_count: {e}")
        return 0


def _container_count() -> int:
    """Count running containers via Docker socket HTTP API."""
    try:
        conn = http.client.HTTPConnection("localhost")
        conn.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        conn.sock.connect("/var/run/docker.sock")
        conn.request("GET", "/containers/json")
        resp = conn.getresponse()
        import json as _json
        data = _json.loads(resp.read())
        conn.close()
        return len(data)
    except Exception as e:
        log.debug(f"container_count: {e}")
        return 0


@router.get("/status")
async def master_status():
    """Live snapshot for helixmaster.millyweb.com. Called by fetchLiveData()."""
    try:
        conn = pg_sync.sqlite_conn()
        try:
            decisions_count = conn.execute(
                "SELECT COUNT(*) FROM structured_archive WHERE collection = %s", ('decisions',)
            ).fetchone()[0]
            kb_count = conn.execute("SELECT COUNT(*) FROM kb_documents").fetchone()[0]
            entity_count = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
            session_count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
            archive_count = conn.execute("SELECT COUNT(*) FROM structured_archive").fetchone()[0]

            recent_rows = conn.execute(
                """
                SELECT content, session_id, created_at, collection
                FROM structured_archive
                ORDER BY created_at DESC
                LIMIT 50
                """
            ).fetchall()
        finally:
            conn.close()

        chunks = _chunk_count()

        db_sizes = {}
        for f in DATA_DIR.glob("*.db"):
            try:
                db_sizes[f.name] = round(f.stat().st_size / 1024 / 1024, 1)
            except Exception:
                pass

        # ADRs from decisions collection
        adrs = []
        adr_idx = 1
        for row in recent_rows:
            content, session, created, collection = row
            if collection != 'decisions': continue
            if adr_idx > 20: break
            title = (content or '').split('\n')[0][:120]
            date_str = str(created)[:10] if created else '?'
            cl = (content or '').lower()
            group = 'General'
            if any(k in cl for k in ['mcp', 'provisioner', 'manifest', 'helix-mcp']): group = 'MCP Architecture'
            elif any(k in cl for k in ['sqlite', 'pg_sync', 'vacuum', 'postgres', 'fts']): group = 'Infrastructure'
            elif any(k in cl for k in ['storage', 'minio', 'write', 'file', 'garage']): group = 'Storage'
            elif any(k in cl for k in ['ingest', 'shard', 'flush', 'chunk']): group = 'Ingestion'
            elif any(k in cl for k in ['haiku', 'observer', 'assembler', 'reconcil']): group = 'Intelligence'
            elif any(k in cl for k in ['compress', 'language', 'phrase']): group = 'Compression'
            elif any(k in cl for k in ['auth', 'tenant', 'login', 'oauth']): group = 'Auth'
            elif any(k in cl for k in ['deploy', 'docker', 'container', 'vps', 'traefik']): group = 'Infrastructure'
            adrs.append({
                'id': f'ADR-{str(adr_idx).zfill(3)}',
                'group': group, 'title': title, 'badge': 'b-d',
                'meta': f"{date_str} \u00b7 {str(session)[:12]}",
            })
            adr_idx += 1

        # Journal: one entry per unique date
        journal = []
        seen_dates = set()
        for row in recent_rows:
            content, session, created, collection = row
            if len(journal) >= 8: break
            date_str = str(created)[:10] if created else '?'
            if date_str in seen_dates: continue
            seen_dates.add(date_str)
            title = (content or '').split('\n')[0][:80]
            journal.append({
                'date': date_str,
                'session': str(session)[:8],
                'title': title,
                'body': (content or '')[:500],
                'decisions': [{'text': title, 'status': 'live'}],
                'build_plan': [],
                'glance': (content or '')[:200],
                'tags': [collection, str(session)[:8]],
            })

        last_session = {
            'date': journal[0]['date'] if journal else str(datetime.now(timezone.utc))[:10],
            'items': [
                f"{decisions_count} decisions recorded",
                f"{entity_count} KG entities tracked",
                f"{session_count} sessions indexed",
                f"{kb_count} KB documents",
            ]
        }

        return JSONResponse({
            'status': 'live',
            'cortex_uptime': _uptime_str(),
            'chunks': chunks,
            'containers': _container_count(),
            'adr_count': decisions_count,
            'counts': {
                'sessions': session_count, 'decisions': decisions_count,
                'entities': entity_count, 'kb_documents': kb_count,
                'archive_total': archive_count, 'chunks': chunks,
            },
            'last_session': last_session,
            'adrs': adrs,
            'journal': journal,
            'db_sizes_mb': db_sizes,
            'generated_at': datetime.now(timezone.utc).isoformat(),
        })

    except Exception as e:
        log.error(f"master_status error: {e}")
        import traceback
        return JSONResponse(
            {'status': 'error', 'error': str(e), 'trace': traceback.format_exc()[-500:], 'cortex_uptime': _uptime_str()},
            status_code=500
        )
