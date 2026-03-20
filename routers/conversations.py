"""Conversations Router — Ingest + Search for Conversation RAG.

Unified conversation pipeline for Helix Cortex:
- Ingest raw transcripts (chunk + embed + FTS5 index)
- Ingest MillyExt extracts
- Hybrid search (vector + BM25 + RRF + temporal decay)
- Stats and management

Also hooks into scan router for code extraction from same transcripts.
"""

import logging
import hashlib
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from services import conversation_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/conversations", tags=["Conversations"])


# === Request/Response Models ===

class IngestRequest(BaseModel):
    """Ingest a raw conversation transcript."""
    text: str = Field(..., description="Raw conversation text")
    session_id: str = Field("", description="Session ID (auto-generated if empty)")
    source: str = Field("claude-ai", description="Source identifier")
    timestamp: str = Field("", description="ISO timestamp of conversation")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Extra metadata")
    scan_code: bool = Field(True, description="Also scan for code blocks (feeds DNA pipeline)")


class ExtractIngestRequest(BaseModel):
    """Ingest a MillyExt Haiku extract."""
    extract: Dict[str, Any] = Field(..., description="MillyExt extract payload")


class BatchIngestRequest(BaseModel):
    """Batch ingest multiple transcripts."""
    items: List[IngestRequest] = Field(..., description="List of transcripts to ingest")


class SearchRequest(BaseModel):
    """Hybrid search query."""
    query: str = Field(..., description="Search query")
    limit: int = Field(5, ge=1, le=20, description="Max results")
    source_filter: Optional[str] = Field(None, description="Filter by source")
    session_filter: Optional[str] = Field(None, description="Filter by session")
    decisions_only: bool = Field(False, description="Only chunks with decisions")
    failures_only: bool = Field(False, description="Only chunks with failures")


# === Routes ===

@router.post("/ingest")
async def ingest_conversation(req: IngestRequest):
    """Ingest a conversation transcript.

    Chunks the text, embeds in ChromaDB, indexes in FTS5.
    Optionally scans for code blocks (feeds DNA pipeline).
    """
    # Auto-generate session_id if not provided
    session_id = req.session_id
    if not session_id:
        h = hashlib.sha256(req.text[:500].encode()).hexdigest()[:12]
        session_id = f"conv-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{h}"

    timestamp = req.timestamp or datetime.now(timezone.utc).isoformat()

    # Ingest into conversation RAG
    result = await conversation_store.ingest_conversation(
        text=req.text,
        session_id=session_id,
        source=req.source,
        timestamp=timestamp,
        metadata=req.metadata,
    )

    # Also scan for code blocks if requested
    code_scan_result = None
    if req.scan_code and req.text:
        try:
            from services.scanner import get_scanner_service
            import re
            pattern = r'```(\w*)\n(.*?)```'
            blocks = re.findall(pattern, req.text, re.DOTALL)
            code_blocks_found = len(blocks)
            if code_blocks_found > 0:
                scanner = get_scanner_service()
                scanned = 0
                for lang, code in blocks:
                    if len(code.strip()) > 50:
                        lang = lang.lower() or "python"
                        if lang in ("python", "py"):
                            atoms = await scanner.extract_atoms(code.strip(), language="python", filepath=f"<transcript:{session_id}>")
                            scanned += len(atoms) if atoms else 0
                code_scan_result = {"blocks_found": code_blocks_found, "atoms_extracted": scanned}
        except Exception as e:
            logger.warning(f"Code scan failed for {session_id}: {e}")
            code_scan_result = {"error": str(e)}

    result["code_scan"] = code_scan_result
    return result


@router.post("/ingest/extract")
async def ingest_extract(req: ExtractIngestRequest):
    """Ingest a MillyExt Haiku extract (pre-processed summary)."""
    return await conversation_store.ingest_extract(req.extract)


@router.post("/ingest/batch")
async def ingest_batch(req: BatchIngestRequest):
    """Batch ingest multiple conversation transcripts."""
    results = []
    for item in req.items:
        session_id = item.session_id
        if not session_id:
            h = hashlib.sha256(item.text[:500].encode()).hexdigest()[:12]
            session_id = f"conv-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{h}"

        timestamp = item.timestamp or datetime.now(timezone.utc).isoformat()

        result = await conversation_store.ingest_conversation(
            text=item.text,
            session_id=session_id,
            source=item.source,
            timestamp=timestamp,
            metadata=item.metadata,
        )
        results.append(result)

    indexed = sum(1 for r in results if r.get("status") == "indexed")
    total_chunks = sum(r.get("chunks", 0) for r in results)

    return {
        "status": "batch_complete",
        "total": len(results),
        "indexed": indexed,
        "total_chunks": total_chunks,
        "results": results,
    }


@router.post("/search")
async def search_conversations(req: SearchRequest):
    """Hybrid search across conversation history.

    Combines ChromaDB vector similarity + FTS5 BM25 keyword matching
    using Reciprocal Rank Fusion with temporal decay.
    """
    if not req.query or not req.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    return await conversation_store.hybrid_search(
        query=req.query,
        limit=req.limit,
        source_filter=req.source_filter,
        session_filter=req.session_filter,
        decisions_only=req.decisions_only,
        failures_only=req.failures_only,
    )


@router.get("/search")
async def search_conversations_get(
    q: str,
    limit: int = 5,
    source: Optional[str] = None,
):
    """GET convenience endpoint for search."""
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="Query parameter 'q' required")

    return await conversation_store.hybrid_search(
        query=q,
        limit=min(limit, 20),
        source_filter=source,
    )


@router.get("/stats")
async def conversation_stats():
    """Get conversation store statistics."""
    return await conversation_store.get_stats()


@router.delete("/{session_id}")
async def delete_conversation(session_id: str):
    """Remove all chunks for a conversation."""
    return await conversation_store.delete_conversation(session_id)
