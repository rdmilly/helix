"""Session Buffer Router — authored session intelligence.

POST   /api/v1/session/buffer          append items (non-blocking)
GET    /api/v1/session/buffer/{sid}    inspect what is buffered
POST   /api/v1/session/close           flush the buffer into the intelligence layer

Contract note: this is deliberately the shape the mesh-controller will inherit
when Helix is absorbed — same field names, same provenance model, so the port is
a move rather than a redesign.
"""
import logging
from typing import Any, Dict

from fastapi import APIRouter, Body, HTTPException

from services import session_buffer

log = logging.getLogger("helix.session_buffer.router")

router = APIRouter(prefix="/api/v1/session")


@router.post("/buffer")
def buffer_append(body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Append authored items to a session buffer. Does no downstream routing."""
    session_id = (body or {}).get("session_id")
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id is required")
    items = (body or {}).get("items")
    if items is None:
        raise HTTPException(status_code=400, detail="items is required (list of {item_type, payload})")
    try:
        session_buffer.ensure_tables()
        return session_buffer.append(
            session_id=session_id,
            items=items,
            project=(body or {}).get("project", "") or "",
            provenance=(body or {}).get("provenance", "authored"),
        )
    except Exception as exc:
        log.exception("session buffer append failed")
        raise HTTPException(status_code=500, detail="buffer append failed: %s" % exc)


@router.get("/buffer/{session_id}")
def buffer_status(session_id: str) -> Dict[str, Any]:
    try:
        session_buffer.ensure_tables()
        return session_buffer.status(session_id)
    except Exception as exc:
        log.exception("session buffer status failed")
        raise HTTPException(status_code=500, detail="buffer status failed: %s" % exc)


@router.post("/close")
def session_close(body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Flush a session's buffer into the durable intelligence layer."""
    session_id = (body or {}).get("session_id")
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id is required")
    try:
        session_buffer.ensure_tables()
        return session_buffer.flush(
            session_id=session_id,
            dry_run=bool((body or {}).get("dry_run", False)),
        )
    except Exception as exc:
        log.exception("session close failed")
        raise HTTPException(status_code=500, detail="session close failed: %s" % exc)
