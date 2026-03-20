"""Synapse Router - Phase 3 API Endpoints

Session lifecycle, search, and context injection.
Replaces Phase 3 stubs in stubs.py.
"""
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from models.synapse import (
    SessionStartRequest, SessionEndRequest,
    AtomSearchRequest, SemanticSearchRequest,
    ContextInjectRequest,
    Tier1Request, Tier1Response,
)
from services.synapse import get_synapse_service

logger = logging.getLogger(__name__)

# === LIFECYCLE ROUTER ===

lifecycle_router = APIRouter(prefix="/api/v1/lifecycle")


@lifecycle_router.post("/session/start")
async def start_session(request: SessionStartRequest):
    """Start a new Helix session with optional context injection."""
    synapse = get_synapse_service()
    
    session = synapse.start_session(
        session_id=request.session_id,
        provider=request.provider,
        model=request.model,
        tags=request.tags,
        meta=request.meta,
    )
    
    context = None
    if request.context_query:
        context = await synapse.assemble_context(
            query=request.context_query,
            session_id=request.session_id,
        )
    
    return {
        "status": "started",
        "session": session,
        "context": context,
    }


@lifecycle_router.post("/session/end")
async def end_session(request: SessionEndRequest):
    """End a Helix session."""
    synapse = get_synapse_service()
    
    session = synapse.end_session(
        session_id=request.session_id,
        summary=request.summary,
        outcome=request.outcome,
    )
    
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {request.session_id} not found")

    try:
        from services.event_bus import publish
        publish("synapse.completed", {
            "session_id": request.session_id,
            "summary": request.summary or "",
            "outcome": request.outcome or "",
            "event": "session_ended",
        })
    except Exception as _be:
        pass

    return {
        "status": "closed",
        "session": session,
    }


@lifecycle_router.get("/session/{session_id}")
async def get_session(session_id: str):
    """Get session details with all meta namespaces."""
    synapse = get_synapse_service()
    session = synapse.get_session(session_id)
    
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
    
    return session


@lifecycle_router.get("/sessions")
async def list_sessions(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    min_significance: int = Query(default=0, ge=0),
):
    """List sessions with optional filtering."""
    synapse = get_synapse_service()
    sessions = synapse.list_sessions(
        limit=limit,
        offset=offset,
        min_significance=min_significance,
    )
    
    return {
        "sessions": sessions,
        "count": len(sessions),
        "limit": limit,
        "offset": offset,
    }


# === SEARCH ROUTER ===

search_router = APIRouter(prefix="/api/v1/search")


@search_router.get("/atoms")
async def search_atoms(
    query: Optional[str] = Query(default=None, description="Text query"),
    name: Optional[str] = Query(default=None, description="Filter by name"),
    category: Optional[str] = Query(default=None, description="Filter by category"),
    language: Optional[str] = Query(default=None, description="Filter by language"),
    min_significance: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
):
    """Search atoms in the DNA library."""
    synapse = get_synapse_service()
    atoms = synapse.search_atoms(
        query=query,
        name=name,
        category=category,
        language=language,
        min_significance=min_significance,
        limit=limit,
    )
    
    return {
        "atoms": atoms,
        "count": len(atoms),
        "filters": {
            "name": name,
            "category": category,
            "language": language,
            "min_significance": min_significance,
        },
    }


@search_router.post("/semantic")
async def semantic_search(request: SemanticSearchRequest):
    """Semantic search across all DNA collections."""
    synapse = get_synapse_service()
    results = await synapse.semantic_search(
        query=request.query,
        collections=request.collections,
        limit=request.limit,
    )
    
    total = sum(len(v) for v in results.values())
    
    return {
        "query": request.query,
        "results": results,
        "total": total,
        "collections_searched": list(results.keys()),
    }


# === CONTEXT INJECTION ROUTER ===

context_router = APIRouter(prefix="/api/v1/context")


@context_router.post("/inject")
async def inject_context(request: ContextInjectRequest):
    """Assemble and inject relevant context for a session."""
    synapse = get_synapse_service()
    context = await synapse.assemble_context(
        query=request.query,
        session_id=request.session_id,
        max_atoms=request.max_atoms,
        max_decisions=request.max_decisions,
        max_sessions=request.max_sessions,
        include_entities=request.include_entities,
        since_session_id=request.since_session_id,
    )

    try:
        from services.event_bus import publish
        publish("synapse.completed", {
            "session_id": request.session_id,
            "query": request.query[:200],
            "atoms_found": len(context.get("atoms", [])) if isinstance(context, dict) else 0,
            "event": "context_injected",
        })
    except Exception as _be:
        pass

    return context


@context_router.get("/session/{session_id}")
async def get_session_context(session_id: str):
    """Get the full assembled context for a specific session."""
    synapse = get_synapse_service()
    session = synapse.get_session(session_id)
    
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
    
    summary = ""
    meta = session.get("meta", {})
    analysis = meta.get("analysis", {})
    if isinstance(analysis, dict):
        summary = analysis.get("summary", "")
    
    context = None
    if summary:
        context = await synapse.assemble_context(
            query=summary,
            session_id=session_id,
            max_atoms=10,
            max_decisions=5,
            max_sessions=3,
        )
    
    return {
        "session": session,
        "context": context,
    }

# === MASTER CONTEXT ROUTER ===

master_router = APIRouter(prefix="/api/v1/master")

import sqlite3
from services import pg_sync
import json
from datetime import datetime, timezone, timedelta
from services.database import get_db_path
from pathlib import Path

FTS_DB_PATH = Path("/opt/projects/helix/data/conversations_fts.db")


@master_router.get("/context")
async def get_master_context(
    days: int = 30,
    top_n: int = 10,
    include_fts: bool = True,
):
    """Assembled master context: top significant sessions + recent FTS chunks."""
    db = get_db_path()
    conn = pg_sync.sqlite_conn(str(db), timeout=10)

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    top_sessions = conn.execute("""
        SELECT id, provider, model, significance, summary, meta, created_at
        FROM sessions
        WHERE created_at >= ? OR significance >= 50
        ORDER BY significance DESC
        LIMIT ?
    """, (cutoff, top_n)).fetchall()
    conn.close()

    sessions_out = []
    for s in top_sessions:
        try: meta = pg_sync.dejson(s["meta"]) if s["meta"] else {}
        except: meta = {}
        custom = meta.get("custom", meta)
        sessions_out.append({
            "id": s["id"],
            "significance": s["significance"],
            "summary": s["summary"] or custom.get("name", ""),
            "has_decision": custom.get("has_decision", False),
            "has_code": custom.get("has_code", False),
            "chunks": custom.get("chunks", 0),
            "created_at": s["created_at"],
        })

    fts_chunks = []
    if include_fts and FTS_DB_PATH.exists():
        try:
            fconn = pg_sync.sqlite_conn(str(FTS_DB_PATH), timeout=10)
            rows = fconn.execute("""
                SELECT c4, c5, c6 FROM conversation_fts_content
                WHERE c6 LIKE '%has_decision": "True%'
                ORDER BY ROWID DESC LIMIT 20
            """).fetchall()
            fconn.close()
            for (topic, text, meta_json) in rows:
                try: m = pg_sync.dejson(meta_json) if meta_json else {}
                except: m = {}
                fts_chunks.append({
                    "session_id": m.get("session_id", ""),
                    "topic": topic[:120] if topic else "",
                    "timestamp": m.get("timestamp", ""),
                    "has_code": m.get("has_code") == "True",
                })
        except Exception as e:
            logger.warning(f"FTS master context error: {e}")

    lines = ["=== MASTER CONTEXT ==="]
    if sessions_out:
        lines.append(f"\nTop {len(sessions_out)} significant sessions:")
        for s in sessions_out[:5]:
            flags = []
            if s["has_decision"]: flags.append("DEC")
            if s["has_code"]: flags.append("CODE")
            label = f"[{s['significance']:.0f}{'|'+','.join(flags) if flags else ''}]"
            lines.append(f"  {label} {s['summary'] or s['id'][:36]}")

    if fts_chunks:
        lines.append(f"\nRecent decisions ({len(fts_chunks)}):")
        for c in fts_chunks[:5]:
            lines.append(f"  [{c['timestamp'][:10]}] {c['topic'][:100]}")

    return {
        "top_sessions": sessions_out,
        "recent_decisions": fts_chunks,
        "injection_text": "\n".join(lines),
        "stats": {
            "sessions": len(sessions_out),
            "recent_decisions": len(fts_chunks),
            "days_window": days,
        }
    }


@master_router.get("/sessions/significant")
async def get_significant_sessions(
    min_significance: float = 50.0,
    limit: int = 20,
):
    """Sessions with significance >= threshold, ordered by score."""
    db = get_db_path()
    conn = pg_sync.sqlite_conn(str(db), timeout=10)
    rows = conn.execute("""
        SELECT id, significance, summary, meta, created_at
        FROM sessions WHERE significance >= ?
        ORDER BY significance DESC LIMIT ?
    """, (min_significance, limit)).fetchall()
    conn.close()

    out = []
    for r in rows:
        try: m = pg_sync.dejson(r["meta"]) if r["meta"] else {}
        except: m = {}
        out.append({
            "id": r["id"],
            "significance": r["significance"],
            "summary": r["summary"] or m.get("custom", m).get("name", ""),
            "meta": m,
            "created_at": r["created_at"],
        })
    return {"sessions": out, "count": len(out), "min_significance": min_significance}


# === TIER 1 ROUTER (Phase 2.1 + 2.2) ===

synapse_tier1_router = APIRouter(prefix="/api/v1/synapse")


@synapse_tier1_router.post("/tier1", response_model=Tier1Response)
async def synapse_tier1(request: Tier1Request):
    """Tier 1 on-demand context enrichment.

    Phase 2.1: 4-store parallel query (atoms, sessions, conversation_chunks,
    kg_neighbors), packs results into the token budget.

    Phase 2.2: Compresses assembled body via server-side shorthand layer
    (~15-25% reduction) before returning. Pass compress=false to skip.

    Returns sandwich-ready injection_text block.
    """
    synapse = get_synapse_service()
    result = await synapse.assemble_tier1(
        query=request.query,
        session_id=request.session_id,
        budget=request.budget,
        compress=request.compress,
    )

    try:
        from services.event_bus import publish
        publish("synapse.tier1", {
            "session_id": request.session_id,
            "query": request.query[:200],
            "tokens_used": result.get("tokens_used", 0),
            "budget": request.budget,
            "compressed": request.compress,
        })
    except Exception:
        pass

    return result


@synapse_tier1_router.get("/tier1/health")
async def synapse_tier1_health():
    """Health check for Tier 1 endpoint."""
    return {"status": "ok", "endpoint": "POST /api/v1/synapse/tier1", "phase": "2.2"}


@synapse_tier1_router.get("/dictionary")
async def synapse_dictionary():
    """Return the compression dictionary for Membrane extension sync.
    
    Returns:
        version: current dictionary version
        entry_count: number of active shorthand symbols
        shorthand_map: {phrase: symbol} for all active symbols
        reverse_map: {symbol: phrase} for expander use
        spec_additions: SPEC block lines to teach LLM current symbols
    """
    from services.phrase_promoter import get_phrase_promoter
    from services.dictionary import get_dictionary_service
    
    d = get_dictionary_service()
    p = get_phrase_promoter()
    
    shorthand_map = p.get_shorthand_map()
    reverse_map = p.get_reverse_map()
    spec_additions = p.build_spec_additions()
    
    return {
        "version": d.version,
        "entry_count": len(shorthand_map),
        "shorthand_map": shorthand_map,
        "reverse_map": reverse_map,
        "spec_additions": spec_additions,
    }
