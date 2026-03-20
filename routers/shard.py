"""Shard Router — Context Sharding API

Endpoints for assembling context shards from diff chains.
Used by session injection, MCP tools, and MemBrain.

GET  /api/v1/context/shard         — assemble shard for one object
GET  /api/v1/context/shard/project — assemble shard for a whole project
POST /api/v1/context/snapshot      — manually take a snapshot
GET  /api/v1/context/diff/{table}/{id} — get raw diff chain
GET  /api/v1/context/maturity/{atom_id} — get maturity score
POST /api/v1/context/snapshots/process — run snapshot queue
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from services.shard import get_shard_assembler, get_snapshot_manager
from services.diff import get_diff_service

router = APIRouter(prefix="/api/v1/context", tags=["context"])


@router.get("/shard")
async def get_shard(
    target_type: str = Query(..., description="atom | session | project"),
    target_id: str = Query(..., description="Object ID"),
    since_session_id: Optional[str] = Query(None, description="Diff since this session ended"),
    since_timestamp: Optional[str] = Query(None, description="ISO timestamp — diff since this time"),
    token_budget: int = Query(2000, description="Max tokens for shard"),
    context_type: str = Query("session_start", description="session_start | inline_suggest | pattern_lookup"),
):
    """Assemble a context shard for an object.

    Returns a delta shard (what changed since last session) or
    a snapshot shard (full current state) depending on how much changed.
    Token cost scales with recency of change, not object size.
    """
    assembler = get_shard_assembler()
    try:
        shard = assembler.assemble_shard(
            target_type=target_type,
            target_id=target_id,
            since_session_id=since_session_id,
            since_timestamp=since_timestamp,
            token_budget=token_budget,
            context_type=context_type,
        )
        return shard
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shard/project")
async def get_project_shard(
    project: str = Query(..., description="Project name"),
    since_session_id: Optional[str] = Query(None),
    token_budget: int = Query(2000),
):
    """Assemble a context shard for an entire project.

    Collects all changed atoms and sessions in the project,
    merges them into a single shard within the token budget.
    """
    assembler = get_shard_assembler()
    try:
        shard = assembler.assemble_project_shard(
            project_name=project,
            since_session_id=since_session_id,
            token_budget=token_budget,
        )
        return shard
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shard/recent")
async def get_recent_shard(
    hours: int = Query(168, description="Look-back window in hours (default 7 days)"),
    token_budget: int = Query(1500, description="Max tokens for shard"),
    include_decisions: bool = Query(True),
    include_entities: bool = Query(True),
    include_atoms: bool = Query(True),
    include_summaries: bool = Query(True),
):
    """Assemble a session-start shard from recent activity.

    No target_id required. Called by MemBrain extension at the start of each
    conversation to prime the session with recent decisions, entities, stable
    code patterns, and session summaries.

    This is the endpoint that closes the Maturation Loop — enriched knowledge
    flows from Cortex back into each new session via injection.
    """
    assembler = get_shard_assembler()
    try:
        shard = assembler.assemble_recent_shard(
            hours=hours,
            token_budget=token_budget,
            include_decisions=include_decisions,
            include_entities=include_entities,
            include_atoms=include_atoms,
            include_summaries=include_summaries,
        )
        return shard
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/diff/{table}/{record_id}")
async def get_diff_chain(
    table: str,
    record_id: str,
    since: Optional[str] = Query(None, description="ISO timestamp"),
    limit: int = Query(50, description="Max diffs to return"),
):
    """Get the raw diff chain for an object.

    Returns diffs in chronological order. Shows full change history
    including reverts, maturity deltas, and diff content.
    """
    valid_tables = {"atoms", "sessions", "molecules", "organisms"}
    if table not in valid_tables:
        raise HTTPException(status_code=400, detail=f"table must be one of: {valid_tables}")

    diff = get_diff_service()
    chain = diff.get_diff_chain(table, record_id, since_timestamp=since, limit=limit)
    return {
        "table": table,
        "record_id": record_id,
        "since": since,
        "diff_count": len(chain),
        "chain": chain,
    }


@router.get("/maturity/{atom_id}")
async def get_atom_maturity(atom_id: str):
    """Get maturity score and history for an atom.

    Maturity: 0.0 (new/unstable) to 1.0 (stable/verified).
    Decreases when code changes. Increases with stable usage.
    """
    diff = get_diff_service()
    score = diff.get_maturity_score(atom_id)
    chain = diff.get_diff_chain("atoms", atom_id, limit=10)

    reverts = [d for d in chain if d.get("is_revert")]
    changes = [d for d in chain if not d.get("is_revert")]

    return {
        "atom_id": atom_id,
        "maturity_score": score,
        "maturity_label": _maturity_label(score),
        "diff_count": len(chain),
        "change_count": len(changes),
        "revert_count": len(reverts),
        "recent_diffs": chain[:5],
    }


@router.post("/snapshot")
async def take_snapshot(
    table: str = Query(...),
    record_id: str = Query(...),
    reason: str = Query("manual"),
):
    """Manually take a base snapshot of an object."""
    valid_tables = {"atoms", "sessions"}
    if table not in valid_tables:
        raise HTTPException(status_code=400, detail=f"table must be one of: {valid_tables}")

    manager = get_snapshot_manager()
    result = manager.take_snapshot(table, record_id, reason=reason)
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return result


@router.post("/snapshots/process")
async def process_snapshot_queue(limit: int = Query(20)):
    """Process the snapshot queue (objects with too many diffs).

    Run this on a schedule (daily cron) or manually to keep
    diff chains short and shard assembly fast.
    """
    manager = get_snapshot_manager()
    result = manager.process_snapshot_queue(limit=limit)
    return result


def _maturity_label(score: float) -> str:
    if score >= 0.85: return "verified"
    if score >= 0.70: return "stable"
    if score >= 0.50: return "maturing"
    if score >= 0.30: return "evolving"
    return "unstable"
