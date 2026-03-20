"""Recovery Router — Context Crash Protection for Helix.

Saves work-state checkpoints and provides resume endpoints so a new
conversation can pick up exactly where the crashed one left off.

Unlike Memory's recovery (which recovers conversation TEXT from the
browser extension), Helix recovery restores WORK STATE: what atoms
were being processed, what's in the queue, what the last operations
were, and what phase/task the build was in.

Endpoints:
  POST /api/v1/recovery/checkpoint     - Save current work state
  GET  /api/v1/recovery/resume          - Get everything needed to resume
  GET  /api/v1/recovery/timeline        - Recent activity feed
  GET  /api/v1/recovery/status          - Quick health + state summary
  DELETE /api/v1/recovery/checkpoint    - Clear checkpoint after clean handoff
"""

import json
import logging
import sqlite3
from services import pg_sync
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from services.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/recovery", tags=["Recovery"])


# ── Models ──────────────────────────────────────────────────

class CheckpointRequest(BaseModel):
    """Save a crash recovery checkpoint."""
    task: str = Field(..., description="What was being worked on")
    status: str = Field(default="in_progress", description="in_progress, blocked, testing, deploying")
    details: str = Field(default="", description="Detailed context about current state")
    files_changed: List[str] = Field(default_factory=list)
    next_steps: List[str] = Field(default_factory=list)
    decisions: List[str] = Field(default_factory=list)
    issues_found: List[str] = Field(default_factory=list)
    conversation_id: Optional[str] = Field(default=None, description="Claude.ai conversation UUID for cross-ref")


class CheckpointResponse(BaseModel):
    saved: bool
    checkpoint_id: str
    timestamp: str


@router.post("/checkpoint", response_model=CheckpointResponse)
async def save_checkpoint(req: CheckpointRequest):
    """Save a crash recovery checkpoint."""
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()
    checkpoint_id = f"chk_{now[:19].replace('-','').replace(':','').replace('T','_')}"

    meta = {
        "task": req.task, "details": req.details, "files_changed": req.files_changed,
        "next_steps": req.next_steps, "decisions": req.decisions,
        "issues_found": req.issues_found, "conversation_id": req.conversation_id,
        "checkpoint_id": checkpoint_id,
    }

    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO project_state (project, status, one_liner, updated_at, meta)
            VALUES ('helix', ?, ?, ?, ?)
            ON CONFLICT(project) DO UPDATE SET
                status=excluded.status, one_liner=excluded.one_liner,
                updated_at=excluded.updated_at, meta=excluded.meta
        """, (req.status, req.task, now, json.dumps(meta)))
        cursor.execute("""
            INSERT INTO meta_events (target_table, target_id, namespace, action, new_value, written_by, timestamp)
            VALUES ('project_state', 'helix', 'recovery', 'checkpoint', ?, 'crash_protection_v1', ?)
        """, (json.dumps(meta), now))
        conn.commit()

    logger.info(f"Recovery checkpoint saved: {req.task} ({req.status})")
    return CheckpointResponse(saved=True, checkpoint_id=checkpoint_id, timestamp=now)


@router.get("/resume")
async def resume():
    """Get everything needed to resume after a context crash."""
    db = get_db()
    with db.get_connection() as conn:
        cursor = conn.cursor()

        # 1. Last checkpoint
        cursor.execute("SELECT project, status, one_liner, updated_at, meta FROM project_state WHERE project = 'helix'")
        row = cursor.fetchone()
        checkpoint = None
        if row:
            meta = pg_sync.dejson(row[4]) if row[4] else {}
            checkpoint = {
                "status": row[1], "task": row[2], "updated_at": row[3],
                "details": meta.get("details", ""), "files_changed": meta.get("files_changed", []),
                "next_steps": meta.get("next_steps", []), "decisions": meta.get("decisions", []),
                "issues_found": meta.get("issues_found", []),
                "conversation_id": meta.get("conversation_id"), "checkpoint_id": meta.get("checkpoint_id"),
            }

        # 2. Timeline
        cursor.execute("SELECT action, target_table, target_id, written_by, timestamp, new_value FROM meta_events ORDER BY timestamp DESC LIMIT 30")
        timeline = []
        for r in cursor.fetchall():
            event = {"action": r[0], "target": f"{r[1]}.{r[2]}" if r[2] else r[1], "by": r[3], "timestamp": r[4]}
            if r[0] == "checkpoint" and r[5]:
                try: event["checkpoint_data"] = pg_sync.dejson(r[5])
                except: pass
            timeline.append(event)

        # 3. Queue
        cursor.execute("SELECT status, COUNT(*) FROM queue GROUP BY status")
        queue_state = {r[0]: r[1] for r in cursor.fetchall()}
        cursor.execute("SELECT id, intake_type, content_type, status, created_at, error FROM queue ORDER BY created_at DESC LIMIT 10")
        recent_queue = [{"id": r[0], "intake_type": r[1], "content_type": r[2], "status": r[3], "created_at": r[4], "error": r[5]} for r in cursor.fetchall()]

        # 4. DNA
        cursor.execute("SELECT COUNT(*) FROM atoms"); atom_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM molecules"); mol_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM organisms"); org_count = cursor.fetchone()[0]
        cursor.execute("SELECT id, name, fp_version, first_seen FROM atoms ORDER BY first_seen DESC LIMIT 10")
        recent_atoms = [{"id": r[0], "name": r[1], "fp_version": r[2], "first_seen": r[3]} for r in cursor.fetchall()]

        # 5. Infrastructure
        cursor.execute("SELECT version, COUNT(*) FROM dictionary_versions GROUP BY version ORDER BY version DESC LIMIT 1")
        dict_row = cursor.fetchone()
        dictionary = {"version": dict_row[0], "entries": dict_row[1]} if dict_row else None
        cursor.execute("SELECT COUNT(*) FROM compression_log"); compression_events = cursor.fetchone()[0]
        cursor.execute("SELECT COALESCE(AVG(compression_ratio_in), 0) FROM compression_log"); avg_ratio = round(cursor.fetchone()[0], 4)
        cursor.execute("SELECT COUNT(*) FROM expressions"); expression_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM sessions"); session_count = cursor.fetchone()[0]
        cursor.execute("SELECT id, summary, created_at FROM sessions ORDER BY created_at DESC LIMIT 5")
        recent_sessions = [{"id": r[0], "summary": r[1], "created_at": r[2]} for r in cursor.fetchall()]

    return {
        "system": "helix-cortex", "resume_time": datetime.now(timezone.utc).isoformat(),
        "checkpoint": checkpoint, "has_checkpoint": checkpoint is not None,
        "timeline": timeline,
        "queue": {"breakdown": queue_state, "recent": recent_queue},
        "dna": {"atoms": atom_count, "molecules": mol_count, "organisms": org_count, "expressions": expression_count, "recent_atoms": recent_atoms},
        "infrastructure": {"dictionary": dictionary, "compression_events": compression_events, "avg_compression_ratio": avg_ratio, "total_sessions": session_count, "recent_sessions": recent_sessions},
    }


@router.get("/timeline")
async def timeline(hours: int = 24, limit: int = 50, action: Optional[str] = None):
    """Get recent activity feed."""
    db = get_db()
    with db.get_connection() as conn:
        cursor = conn.cursor()
        where_parts = [f"timestamp > datetime('now', '-{int(hours)} hours')"]
        params = []
        if action:
            where_parts.append("action = ?"); params.append(action)
        where_sql = " AND ".join(where_parts)
        cursor.execute(f"SELECT action, target_table, target_id, namespace, old_value, new_value, written_by, timestamp FROM meta_events WHERE {where_sql} ORDER BY timestamp DESC LIMIT ?", params + [limit])
        events = []
        for r in cursor.fetchall():
            event = {"action": r[0], "target_table": r[1], "target_id": r[2], "namespace": r[3], "written_by": r[6], "timestamp": r[7]}
            if r[0] in ("checkpoint", "create", "update", "error") and r[5]:
                try: event["new_value"] = pg_sync.dejson(r[5])
                except: event["new_value"] = r[5][:500] if r[5] else None
            events.append(event)
        cursor.execute(f"SELECT action, COUNT(*) FROM meta_events WHERE {where_sql} GROUP BY action", params)
        summary = {r[0]: r[1] for r in cursor.fetchall()}
    return {"hours": hours, "event_count": len(events), "summary": summary, "events": events}


@router.get("/status")
async def status():
    """Quick status check."""
    db = get_db()
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT status, one_liner, updated_at, meta FROM project_state WHERE project = 'helix'")
        row = cursor.fetchone()
        has_checkpoint = row is not None
        checkpoint_info = None
        if row:
            meta = pg_sync.dejson(row[3]) if row[3] else {}
            checkpoint_info = {"status": row[0], "task": row[1], "updated_at": row[2], "next_steps": meta.get("next_steps", []), "issues_found": meta.get("issues_found", []), "conversation_id": meta.get("conversation_id")}
        cursor.execute("SELECT COUNT(*) FROM meta_events"); total_events = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(timestamp) FROM meta_events"); last_activity = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM queue WHERE status IN ('pending', 'processing')"); pending_work = cursor.fetchone()[0]
    return {"has_checkpoint": has_checkpoint, "checkpoint": checkpoint_info, "total_events": total_events, "last_activity": last_activity, "pending_queue_items": pending_work}


@router.delete("/checkpoint")
async def clear_checkpoint():
    """Clear checkpoint after clean handoff."""
    db = get_db()
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM project_state WHERE project = 'helix'")
        now = datetime.now(timezone.utc).isoformat()
        cursor.execute("INSERT INTO meta_events (target_table, target_id, namespace, action, written_by, timestamp) VALUES ('project_state', 'helix', 'recovery', 'checkpoint_cleared', 'crash_protection_v1', ?)", (now,))
        conn.commit()
    return {"cleared": True, "timestamp": now}
