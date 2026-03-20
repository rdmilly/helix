"""
MemBrain Vector Service
Per-user ChromaDB collections for the paid vector tier.

Each paying user gets their own ChromaDB collection:
    membrain_{user_id}   e.g. membrain_usr_abc123def456

This is entirely separate from Helix's own collections
(helix_atoms_*, helix_sessions_*, etc.).

Embedding model: bge-large-en-v1.5 (1024d)
  - Same fastembed model already running in Cortex
  - Superior to the extension's local all-MiniLM-L6-v2 (384d)
  - Already warmed up at startup — zero cold-start cost

Usage:
    from services.membrain_vector import membrain_vector
    await membrain_vector.ensure_collection(user_id)
    await membrain_vector.upsert(user_id, fact_id, text, metadata)
    results = await membrain_vector.search(user_id, query, top_k=8)
    await membrain_vector.delete(user_id, fact_id)
"""
import logging
from typing import Any, Dict, List, Optional

import httpx

from config import CHROMADB_HOST, CHROMADB_PORT, EMBEDDING_DIMENSIONS
from services.embeddings import get_embedding_service

logger = logging.getLogger(__name__)

# ChromaDB REST base
CHROMA_BASE = f"http://{CHROMADB_HOST}:{CHROMADB_PORT}"

# Distance threshold for ChromaDB (L2 distance, not cosine).
# ChromaDB returns L2 distance — lower is more similar.
# ~0.8 L2 distance ≈ ~0.35 cosine similarity for normalized BGE vectors.
DEFAULT_DISTANCE_THRESHOLD = 1.2


class MembrainVectorService:
    """
    Manages per-user vector collections in ChromaDB.

    All operations are async (httpx). Embeddings computed synchronously
    via fastembed before the HTTP call (same pattern as ChromaDBService).
    """

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None
        # Cache collection UUIDs: user_id -> chroma_collection_id
        self._collection_cache: Dict[str, str] = {}

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=CHROMA_BASE,
                timeout=httpx.Timeout(15.0),
            )
        return self._client

    # ==================== COLLECTION MANAGEMENT ====================

    async def ensure_collection(self, user_id: str) -> Optional[str]:
        """
        Get or create a ChromaDB collection for this user.
        Returns the ChromaDB collection UUID, or None on failure.
        Called at migration time and lazily before any upsert.
        """
        collection_name = f"membrain_{user_id}"

        # Return cached UUID if we have it
        if user_id in self._collection_cache:
            return self._collection_cache[user_id]

        try:
            client = await self._get_client()
            resp = await client.post("/api/v1/collections", json={
                "name": collection_name,
                "metadata": {
                    "embedding_model": "bge-large-en-v1.5",
                    "dimensions": str(EMBEDDING_DIMENSIONS),
                    "created_by": "membrain_vector_service",
                    "user_id": user_id,
                },
                "get_or_create": True,
            })

            if resp.status_code == 200:
                data = resp.json()
                chroma_id = data.get("id", "")
                self._collection_cache[user_id] = chroma_id
                logger.info(f"[MembrainVector] Collection ready: {collection_name} ({chroma_id[:8]}…)")
                return chroma_id
            else:
                logger.error(f"[MembrainVector] Failed to create collection {collection_name}: {resp.status_code} {resp.text[:200]}")
                return None

        except Exception as e:
            logger.error(f"[MembrainVector] ensure_collection failed: {e}")
            return None

    async def delete_collection(self, user_id: str) -> bool:
        """
        Delete a user's entire collection (e.g., account deletion).
        Irreversible — clears all vectors.
        """
        collection_name = f"membrain_{user_id}"
        try:
            client = await self._get_client()
            resp = await client.delete(f"/api/v1/collections/{collection_name}")
            self._collection_cache.pop(user_id, None)
            logger.info(f"[MembrainVector] Deleted collection: {collection_name}")
            return resp.status_code in (200, 404)
        except Exception as e:
            logger.error(f"[MembrainVector] delete_collection failed: {e}")
            return False

    # ==================== UPSERT ====================

    async def upsert(
        self,
        user_id: str,
        fact_id: str,
        text: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Embed and store a single fact.
        Idempotent — called both during migration and for new facts post-upgrade.
        """
        chroma_id = await self.ensure_collection(user_id)
        if not chroma_id:
            return False

        embedder = get_embedding_service()
        if not embedder.is_ready:
            logger.warning("[MembrainVector] Embedder not ready, skipping upsert")
            return False

        embedding = embedder.embed_single(text[:8000])
        if not embedding:
            logger.warning(f"[MembrainVector] Embedding failed for fact {fact_id}")
            return False

        clean_meta = _clean_metadata({
            "user_id": user_id,
            "fact_id": fact_id,
            **(metadata or {}),
        })

        try:
            client = await self._get_client()
            resp = await client.post(
                f"/api/v1/collections/{chroma_id}/upsert",
                json={
                    "ids": [fact_id],
                    "documents": [text[:8000]],
                    "embeddings": [embedding],
                    "metadatas": [clean_meta],
                },
            )

            if resp.status_code == 200:
                return True
            else:
                logger.warning(f"[MembrainVector] Upsert failed: {resp.status_code} {resp.text[:200]}")
                return False

        except Exception as e:
            logger.error(f"[MembrainVector] Upsert exception: {e}")
            return False

    async def upsert_batch(
        self,
        user_id: str,
        facts: List[Dict[str, Any]],
    ) -> Dict[str, int]:
        """
        Bulk upsert for migration. facts is a list of { factId, text, category, ... }.
        Returns { "upserted": N, "failed": M }.
        """
        chroma_id = await self.ensure_collection(user_id)
        if not chroma_id:
            return {"upserted": 0, "failed": len(facts)}

        embedder = get_embedding_service()
        if not embedder.is_ready:
            return {"upserted": 0, "failed": len(facts)}

        # Embed all texts in a single fastembed batch call (efficient)
        texts = [f.get("text") or f.get("content", "") for f in facts]
        embeddings = embedder.embed_texts([t[:8000] for t in texts])

        if len(embeddings) != len(facts):
            logger.error(f"[MembrainVector] Embedding count mismatch: {len(embeddings)} != {len(facts)}")
            return {"upserted": 0, "failed": len(facts)}

        ids = [f.get("factId") or f.get("id", f"fact-{i}") for i, f in enumerate(facts)]
        documents = texts
        metadatas = [
            _clean_metadata({
                "user_id": user_id,
                "category": f.get("category", ""),
                "confidence": f.get("confidence", ""),
                "source": "migration",
            })
            for f in facts
        ]

        # ChromaDB upsert is idempotent — safe to batch all at once
        # Max batch size ~5000; migration is rarely > 200 facts per user
        try:
            client = await self._get_client()
            resp = await client.post(
                f"/api/v1/collections/{chroma_id}/upsert",
                json={
                    "ids": ids,
                    "documents": documents,
                    "embeddings": embeddings,
                    "metadatas": metadatas,
                },
                timeout=httpx.Timeout(60.0),  # Longer timeout for large batches
            )

            if resp.status_code == 200:
                logger.info(f"[MembrainVector] Batch upsert: {len(facts)} facts for {user_id}")
                return {"upserted": len(facts), "failed": 0}
            else:
                logger.warning(f"[MembrainVector] Batch upsert failed: {resp.status_code}")
                return {"upserted": 0, "failed": len(facts)}

        except Exception as e:
            logger.error(f"[MembrainVector] Batch upsert exception: {e}")
            return {"upserted": 0, "failed": len(facts)}

    # ==================== SEARCH ====================

    async def search(
        self,
        user_id: str,
        query: str,
        top_k: int = 8,
        threshold: float = 0.35,
    ) -> List[Dict[str, Any]]:
        """
        Semantic search in a user's collection.

        threshold is cosine similarity (0–1). We convert to L2 distance
        internally since ChromaDB's default space is L2.

        Returns [{ factId, score, text }, ...] sorted by score desc.
        """
        chroma_id = await self.ensure_collection(user_id)
        if not chroma_id:
            return []

        embedder = get_embedding_service()
        if not embedder.is_ready:
            return []

        query_embedding = embedder.embed_single(query[:2000])
        if not query_embedding:
            return []

        try:
            client = await self._get_client()
            resp = await client.post(
                f"/api/v1/collections/{chroma_id}/query",
                json={
                    "query_embeddings": [query_embedding],
                    "n_results": top_k,
                    "include": ["documents", "metadatas", "distances"],
                },
            )

            if resp.status_code != 200:
                logger.warning(f"[MembrainVector] Search failed: {resp.status_code}")
                return []

            data = resp.json()
            results = []

            if data.get("ids") and data["ids"][0]:
                for i, fact_id in enumerate(data["ids"][0]):
                    distance = data["distances"][0][i] if data.get("distances") else 1.0
                    # Convert L2 distance → approximate cosine similarity
                    # For normalized BGE vectors: cosine_sim ≈ 1 - (distance² / 2)
                    score = max(0.0, 1.0 - (distance ** 2) / 2)

                    if score < threshold:
                        continue

                    results.append({
                        "factId": fact_id,
                        "score": round(score, 4),
                        "text": data["documents"][0][i] if data.get("documents") else "",
                    })

            # Already sorted by distance (ascending) = score descending
            return results

        except Exception as e:
            logger.error(f"[MembrainVector] Search exception: {e}")
            return []

    # ==================== DELETE ====================

    async def delete(self, user_id: str, fact_id: str) -> bool:
        """Remove a single fact's vector from the user's collection."""
        chroma_id = await self.ensure_collection(user_id)
        if not chroma_id:
            return False

        try:
            client = await self._get_client()
            resp = await client.post(
                f"/api/v1/collections/{chroma_id}/delete",
                json={"ids": [fact_id]},
            )
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"[MembrainVector] Delete exception: {e}")
            return False

    # ==================== STATS ====================

    async def get_collection_stats(self, user_id: str) -> Dict[str, Any]:
        """Get count of vectors in a user's collection."""
        chroma_id = await self.ensure_collection(user_id)
        if not chroma_id:
            return {"count": 0, "error": "collection not found"}

        try:
            client = await self._get_client()
            resp = await client.get(f"/api/v1/collections/{chroma_id}/count")
            if resp.status_code == 200:
                count = resp.json()
                return {"count": count, "collection": f"membrain_{user_id}"}
            return {"count": 0}
        except Exception as e:
            return {"count": 0, "error": str(e)}


# ==================== HELPERS ====================

def _clean_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """ChromaDB only accepts str, int, float, bool in metadata."""
    import json as _json
    clean = {}
    for k, v in metadata.items():
        if isinstance(v, (str, int, float, bool)):
            clean[k] = v
        elif v is None:
            clean[k] = ""
        else:
            clean[k] = _json.dumps(v)
    return clean


# Singleton
membrain_vector = MembrainVectorService()
