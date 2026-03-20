"""Conversation Store — Ingest + Hybrid Search for Helix Cortex.

Ported from Memory v0.5. Combines:
- ChromaDB vector search (semantic similarity) via Helix's async ChromaDB service
- SQLite FTS5 BM25 search (keyword matching)
- Reciprocal Rank Fusion (RRF) for score combination
- Temporal decay for recency weighting

Ingest path:
  Raw text -> Chunker -> ChromaDB + FTS5

Search path:
  Query -> [ChromaDB, FTS5] -> RRF merge -> temporal decay -> ranked results
"""

import math
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from config import (
    RAG_RESULTS_DEFAULT, RAG_RESULTS_MAX,
    RAG_VECTOR_WEIGHT, RAG_BM25_WEIGHT, RAG_RRF_K,
    TEMPORAL_DECAY_ENABLED, TEMPORAL_DECAY_HALF_LIFE_DAYS, TEMPORAL_DECAY_MIN_FACTOR,
)
from services.chunker import chunk_conversation, chunk_extract, ChunkResult
from services.chromadb import get_chromadb_service
from services import bm25_store

logger = logging.getLogger(__name__)


# ============================================================
#  INGEST
# ============================================================

async def ingest_conversation(
    text: str,
    session_id: str,
    source: str = "claude-ai",
    timestamp: str = "",
    metadata: Dict = None,
) -> Dict[str, Any]:
    """Ingest a raw conversation transcript.

    Chunks the text and indexes in both ChromaDB and FTS5.
    Returns ingest stats.
    """
    if not text or not text.strip():
        return {"status": "skipped", "reason": "empty text"}

    result = chunk_conversation(
        text=text,
        session_id=session_id,
        source=source,
        timestamp=timestamp,
    )

    return await _index_chunks(result, metadata)


async def ingest_extract(extract: Dict) -> Dict[str, Any]:
    """Ingest a MillyExt Haiku extract."""
    if not extract:
        return {"status": "skipped", "reason": "empty extract"}

    result = chunk_extract(extract)
    meta = {
        "name": extract.get("name", ""),
        "topics": json.dumps(extract.get("topics", [])),
        "model": extract.get("model", ""),
        "message_count": str(extract.get("message_count", 0)),
    }
    return await _index_chunks(result, meta)


async def _index_chunks(result: ChunkResult, extra_meta: Dict = None) -> Dict[str, Any]:
    """Index chunks into both ChromaDB and FTS5."""
    chromadb = get_chromadb_service()
    chromadb_count = 0
    fts_count = 0
    fts_batch = []

    for chunk in result.chunks:
        chunk_id = f"{result.session_id}:chunk-{chunk.chunk_index}"

        # ChromaDB metadata
        meta = {
            "session_id": result.session_id,
            "chunk_index": str(chunk.chunk_index),
            "source": chunk.source,
            "timestamp": chunk.timestamp,
            "topic_hint": chunk.topic_hint,
            "has_decision": str(chunk.has_decision),
            "has_failure": str(chunk.has_failure),
            "has_code": str(chunk.has_code),
            "char_count": str(chunk.char_count),
            "strategy": result.strategy,
        }
        if extra_meta:
            for k, v in extra_meta.items():
                if isinstance(v, (str, int, float, bool)):
                    meta[k] = str(v)

        # ChromaDB vector index (async)
        if await chromadb.add_document(
            "conversations", chunk_id, chunk.text, meta
        ):
            chromadb_count += 1

        # FTS5 batch
        fts_batch.append({
            "chunk_id": chunk_id,
            "session_id": result.session_id,
            "source": chunk.source,
            "timestamp": chunk.timestamp,
            "topic_hint": chunk.topic_hint,
            "content": chunk.text,
            "metadata": meta,
        })

    # Batch FTS5 index (sync but fast)
    fts_count = bm25_store.index_batch(fts_batch)

    logger.info(
        f"Indexed {result.session_id}: {chromadb_count} vector + {fts_count} FTS "
        f"({len(result.chunks)} chunks, {result.total_chars} chars)"
    )

    return {
        "status": "indexed",
        "session_id": result.session_id,
        "chunks": len(result.chunks),
        "chromadb_indexed": chromadb_count,
        "fts_indexed": fts_count,
        "total_chars": result.total_chars,
        "strategy": result.strategy,
    }


# ============================================================
#  SEARCH
# ============================================================

async def hybrid_search(
    query: str,
    limit: int = RAG_RESULTS_DEFAULT,
    source_filter: Optional[str] = None,
    session_filter: Optional[str] = None,
    decisions_only: bool = False,
    failures_only: bool = False,
) -> Dict[str, Any]:
    """Hybrid search combining vector similarity and keyword matching.

    Uses Reciprocal Rank Fusion (RRF) to combine rankings.
    """
    limit = min(limit, RAG_RESULTS_MAX)
    fetch_limit = limit * 3

    # 1. Vector search via ChromaDB (async)
    chromadb = get_chromadb_service()
    where_filter = {}
    if source_filter:
        where_filter["source"] = source_filter
    if decisions_only:
        where_filter["has_decision"] = "True"
    if failures_only:
        where_filter["has_failure"] = "True"

    vector_results = await chromadb.search_similar(
        query,
        collection_base="conversations",
        limit=fetch_limit,
        where=where_filter if where_filter else None,
    )

    # Convert to common format
    vector_hits = []
    for r in vector_results:
        vector_hits.append({
            "id": r.get("id", ""),
            "content": r.get("document", ""),
            "metadata": r.get("metadata", {}),
            "distance": r.get("distance"),
        })

    # 2. BM25 keyword search (sync)
    bm25_hits = bm25_store.search(
        query,
        limit=fetch_limit,
        session_filter=session_filter,
    )

    # 3. RRF fusion
    fused = _rrf_fuse(vector_hits, bm25_hits, limit)

    # 4. Temporal decay
    fused = _apply_temporal_decay(fused)

    return {
        "query": query,
        "results": fused,
        "total_results": len(fused),
        "vector_hits": len(vector_hits),
        "bm25_hits": len(bm25_hits),
        "fusion": "rrf",
    }


def _rrf_fuse(
    vector_hits: List[Dict],
    bm25_hits: List[Dict],
    limit: int,
) -> List[Dict]:
    """Reciprocal Rank Fusion.
    score = w_v * 1/(k + rank_v) + w_b * 1/(k + rank_b)
    """
    k = RAG_RRF_K
    w_v = RAG_VECTOR_WEIGHT
    w_b = RAG_BM25_WEIGHT

    scores = {}

    for rank, hit in enumerate(vector_hits):
        cid = hit.get("id", "")
        if not cid:
            continue
        rrf_score = w_v * (1.0 / (k + rank + 1))
        scores[cid] = {
            "score": rrf_score,
            "content": hit.get("content", ""),
            "metadata": hit.get("metadata", {}),
            "vector_rank": rank + 1,
            "vector_distance": hit.get("distance"),
            "bm25_rank": None,
        }

    for rank, hit in enumerate(bm25_hits):
        cid = hit.get("chunk_id", "")
        if not cid:
            continue
        rrf_score = w_b * (1.0 / (k + rank + 1))

        if cid in scores:
            scores[cid]["score"] += rrf_score
            scores[cid]["bm25_rank"] = rank + 1
            scores[cid]["snippet"] = hit.get("snippet", "")
        else:
            scores[cid] = {
                "score": rrf_score,
                "content": hit.get("content", ""),
                "metadata": hit.get("metadata", {}),
                "vector_rank": None,
                "vector_distance": None,
                "bm25_rank": rank + 1,
                "snippet": hit.get("snippet", ""),
            }

    ranked = sorted(scores.items(), key=lambda x: x[1]["score"], reverse=True)

    results = []
    for cid, data in ranked[:limit]:
        meta = data.get("metadata", {})
        results.append({
            "chunk_id": cid,
            "session_id": meta.get("session_id", cid.split(":")[0] if ":" in cid else ""),
            "content": data["content"],
            "score": round(data["score"], 6),
            "topic_hint": meta.get("topic_hint", ""),
            "source": meta.get("source", ""),
            "timestamp": meta.get("timestamp", ""),
            "has_decision": meta.get("has_decision", "False") == "True",
            "has_failure": meta.get("has_failure", "False") == "True",
            "has_code": meta.get("has_code", "False") == "True",
            "vector_rank": data.get("vector_rank"),
            "bm25_rank": data.get("bm25_rank"),
            "snippet": data.get("snippet", ""),
        })

    return results


def _apply_temporal_decay(results: List[Dict]) -> List[Dict]:
    """Apply time-based decay to search scores.
    More recent conversations get higher effective scores.
    Uses exponential decay: factor = max(min_factor, 0.5^(age_days / half_life))
    """
    if not TEMPORAL_DECAY_ENABLED or not results:
        return results

    now = datetime.now(timezone.utc)
    half_life = TEMPORAL_DECAY_HALF_LIFE_DAYS
    min_factor = TEMPORAL_DECAY_MIN_FACTOR

    for r in results:
        ts = r.get("timestamp", "")
        if not ts:
            r["temporal_factor"] = min_factor
            r["score"] = r["score"] * min_factor
            continue

        try:
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            created = datetime.fromisoformat(ts)
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)

            age_days = (now - created).total_seconds() / 86400
            factor = max(min_factor, math.pow(0.5, age_days / half_life))
        except Exception:
            factor = min_factor

        r["temporal_factor"] = round(factor, 4)
        r["score"] = round(r["score"] * factor, 6)

    results.sort(key=lambda x: x.get("score", 0), reverse=True)
    return results


# ============================================================
#  STATS
# ============================================================

async def get_stats() -> Dict:
    """Get conversation store statistics."""
    try:
        chromadb = get_chromadb_service()
        # Get chroma count via search with empty-ish query
        # For now just report FTS stats which are more reliable
        fts_stats = bm25_store.get_stats()

        return {
            "fts_chunks": fts_stats.get("total_chunks", 0),
            "fts_sessions": fts_stats.get("unique_sessions", 0),
            "fts_db_size_mb": fts_stats.get("db_size_mb", 0),
        }
    except Exception as e:
        return {"error": str(e)}


async def delete_conversation(session_id: str) -> Dict:
    """Remove all chunks for a conversation from both indexes."""
    # FTS5 delete
    fts_deleted = bm25_store.delete_session(session_id)

    # TODO: ChromaDB delete by session_id filter
    # Helix ChromaDB service needs a delete method added

    return {
        "session_id": session_id,
        "fts_deleted": fts_deleted,
    }
