"""Exchange Router — Per-exchange structured observations.

Endpoints:
  POST /api/v1/exchange/post    — Record a structured exchange observation
  GET  /api/v1/exchange/search  — Search past exchanges
  GET  /api/v1/exchange/stats   — Exchange statistics
"""
from typing import Optional, List
from pydantic import BaseModel, Field
from fastapi import APIRouter, Query

from services.exchange import record_exchange, search_exchanges, get_exchange_stats

router = APIRouter(prefix="/api/v1/exchange")


class ExchangePost(BaseModel):
    session_id: str = "unknown"
    exchange_num: int = 0

    # What changed
    exchange_type: str = "discuss"   # build, debug, plan, discuss, research, review, deploy
    project: str = ""
    domain: str = ""                  # infra, code, business, content, personal
    files_changed: List[str] = []
    services_changed: List[str] = []
    state_before: str = ""
    state_after: str = ""

    # Why (reasoning)
    decision: str = ""
    reason: str = ""
    rejected_alternatives: str = ""
    constraint_discovered: str = ""

    # What was learned
    failure: str = ""
    pattern: str = ""
    entities_mentioned: list = []     # [{"name": "...", "type": "...", "description": "..."}]
    relationships_found: list = []    # [{"source": "...", "target": "...", "type": "..."}]

    # Forward-looking
    next_step: str = ""
    open_questions: List[str] = []
    confidence: float = 0.7

    # Session context
    session_summary: str = ""     # Running narrative of what this session accomplished
    session_goals: list = []      # What we set out to do
    actions_taken: list = []      # Actions to execute: [{"type": "update_handoff"}, {"type": "write_journal", "entry": "..."}]

    # Meta
    skip: bool = False
    tool_calls: int = 0
    tools_used: List[str] = []
    complexity: str = "low"           # low, medium, high
    what_happened: str = ""
    notes: str = ""


@router.post("/post")
async def post_exchange(data: ExchangePost):
    """Record a structured exchange observation.

    Called after every Claude response. Auto-routes intelligence:
    - Decisions → structured archive
    - Failures → structured archive
    - Patterns/constraints → structured archive
    - Entities → knowledge graph
    - Relationships → knowledge graph
    """
    return record_exchange(data.model_dump())


@router.get("/search")
async def search(
    q: str = Query("", description="FTS search query"),
    project: str = Query(""),
    exchange_type: str = Query(""),
    limit: int = Query(20, ge=1, le=100),
):
    return search_exchanges(q, project, exchange_type, limit)


@router.get("/stats")
async def stats():
    return get_exchange_stats()
