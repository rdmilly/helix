"""Turn Flush Router — Inbound per-turn flush from MemBrain

Receives turn data from the browser extension after each stream_complete.
Publishes turn.flush event to the worker queue for fan-out processing.

POST /api/v1/turn/flush
"""
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

log = logging.getLogger("helix.routers.turn_flush")

router = APIRouter(prefix="/api/v1/turn", tags=["turn"])


class TurnMessage(BaseModel):
    role: str
    content: Any  # str or list (structured content)


class TurnFlushRequest(BaseModel):
    session_id: str = Field(..., description="Helix session ID (or claude.ai convo UUID)")
    turn_index: int = Field(default=0, description="Turn number within the session")
    messages: List[TurnMessage] = Field(default=[], description="Messages for this turn")
    provider: str = Field(default="anthropic")
    model: str = Field(default="unknown")
    prev_session_id: Optional[str] = Field(default=None)


@router.post("/flush")
async def turn_flush(request: TurnFlushRequest):
    """Receive a completed turn from MemBrain and fan it out to all processors.

    Called by the MillyExt content script after each stream_complete event.
    Immediately publishes turn.flush to the worker queue and returns.
    Processing is async — caller does not wait for fan-out to complete.
    """
    try:
        from services.event_bus import publish
        evt_id = publish(
            "turn.flush",
            {
                "session_id": request.session_id,
                "turn_index": request.turn_index,
                "messages": [
                    {"role": m.role, "content": m.content}
                    for m in request.messages
                ],
                "provider": request.provider,
                "model": request.model,
                "prev_session_id": request.prev_session_id,
            },
        )
        log.info(
            f"turn.flush queued: {evt_id} session={request.session_id} "
            f"turn={request.turn_index} msgs={len(request.messages)}"
        )
        return {
            "status": "queued",
            "event_id": evt_id,
            "session_id": request.session_id,
            "turn_index": request.turn_index,
        }
    except Exception as e:
        log.error(f"turn.flush publish failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/flush/health")
async def turn_flush_health():
    """Health check for the turn flush endpoint."""
    return {"status": "ok", "endpoint": "turn_flush_v1"}
