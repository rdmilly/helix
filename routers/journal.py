"""Session Journal Router — /api/v1/journal/"""
from __future__ import annotations
import json, uuid
from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, Query
from pydantic import BaseModel, Field
from services.pg_sync import get_pg_conn

router = APIRouter(prefix="/api/v1/journal", tags=["journal"])


class JournalWriteRequest(BaseModel):
    topic: str = Field(..., min_length=1, max_length=50)
    session_id: str = ""
    node: str = "vps1"
    completed: List[str] = Field(default_factory=list)
    in_progress: List[str] = Field(default_factory=list)
    blocked: List[dict] = Field(default_factory=list)
    next_session: str = ""
    warnings: str = ""
    raw_handoff: str = ""


@router.post("/write")
async def journal_write(req: JournalWriteRequest):
    """Write a session journal entry. Call at END of every session."""
    entry_id = str(uuid.uuid4())
    sid = req.session_id or f"{req.topic}-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M')}"
    with get_pg_conn() as conn:
        conn.execute(
            "INSERT INTO session_journal (id,session_id,topic,node,completed,in_progress,blocked,next_session,warnings,raw_handoff,ts) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())",
            (entry_id, sid, req.topic, req.node,
             json.dumps(req.completed), json.dumps(req.in_progress), json.dumps(req.blocked),
             req.next_session, req.warnings, req.raw_handoff)
        )
        conn.commit()
    return {"id": entry_id, "session_id": sid, "ts": datetime.now(timezone.utc).isoformat(), "status": "written"}


@router.get("/read")
async def journal_read(
    last: int = Query(default=5, ge=1, le=20),
    topic: Optional[str] = Query(default=None),
    warnings_only: bool = Query(default=False),
):
    """Read recent journal entries. Call at START of every session."""
    conds, params = [], []
    if topic:
        conds.append("topic = %s"); params.append(topic)
    if warnings_only:
        conds.append("warnings != ''")
    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    params.append(min(last, 20))

    with get_pg_conn() as conn:
        rows = conn.execute(
            f"SELECT id,ts,session_id,topic,node,completed,in_progress,blocked,next_session,warnings,raw_handoff FROM session_journal {where} ORDER BY ts DESC LIMIT %s",
            params
        ).fetchall()

    def _j(v):
        if isinstance(v, str):
            try: return json.loads(v)
            except: return v
        return v or []

    entries = [{"id":r[0],"ts":r[1].isoformat() if r[1] else None,"session_id":r[2],"topic":r[3],"node":r[4],
                "completed":_j(r[5]),"in_progress":_j(r[6]),"blocked":_j(r[7]),
                "next_session":r[8],"warnings":r[9],"raw_handoff":r[10]} for r in rows]
    return {"count": len(entries), "entries": entries,
            "tip": "Check 'warnings' on each entry first — cross-session alerts live there."}
