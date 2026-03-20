"""VectorStore — pgvector + helix-embeddings sidecar.

Drop-in replacement for ChromaDBService.
Same public API:
  add_document(collection_base, doc_id, text, metadata) -> bool
  search_similar(query, collection_base, limit, where)   -> List[Dict]
  health_check()                                         -> bool

Backed by:
  - helix-embeddings HTTP sidecar (BGE-large-en-v1.5, dim=1024)
  - helix-postgres `embeddings` table with pgvector ivfflat index
"""
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import httpx
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

EMBEDDINGS_URL = os.getenv("EMBEDDINGS_URL", "http://helix-embeddings:8000")
POSTGRES_DSN   = os.getenv(
    "POSTGRES_DSN",
    "host=helix-postgres user=helix password=934d69eb7ce6a90710643e93efe36fcc dbname=helix"
)


class CircuitBreaker:
    def __init__(self, threshold: int = 3, timeout: int = 120):
        self.threshold = threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.is_open = False

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.threshold:
            self.is_open = True

    def record_success(self):
        self.failure_count = 0
        self.is_open = False

    def can_execute(self) -> bool:
        if not self.is_open:
            return True
        return (time.time() - self.last_failure_time) > self.timeout


class VectorStore:
    """
    pgvector-backed vector store with the same interface as ChromaDBService.

    Collections map to source_type in the `embeddings` table.
    Recognised source types: atoms, sessions, entities, conversations, intelligence.
    """

    def __init__(self):
        self.circuit_breaker = CircuitBreaker()
        self._initialized = False
        self._client: Optional[httpx.AsyncClient] = None
        self.current_model = "bge-large-en-v1.5"

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=EMBEDDINGS_URL,
                timeout=httpx.Timeout(15.0),
            )
        return self._client

    async def initialize(self) -> bool:
        """Verify connectivity to embeddings sidecar and postgres."""
        if not self.circuit_breaker.can_execute():
            return False
        try:
            client = await self._get_client()
            resp = await client.get("/health")
            if resp.status_code != 200:
                raise ConnectionError(f"embeddings sidecar unhealthy: {resp.status_code}")
            data = resp.json()
            logger.info(f"VectorStore ready: embeddings={data.get('model')} dim={data.get('dim', 1024)}")
            # Quick postgres connectivity check
            conn = psycopg2.connect(POSTGRES_DSN, connect_timeout=5)
            conn.close()
            self.circuit_breaker.record_success()
            self._initialized = True
            return True
        except Exception as e:
            logger.error(f"VectorStore init failed: {e}")
            self.circuit_breaker.record_failure()
            return False

    async def _embed(self, text: str) -> Optional[List[float]]:
        """Get embedding vector from sidecar."""
        try:
            client = await self._get_client()
            resp = await client.post("/embed", json={"texts": [text[:8000]], "normalize": True})
            if resp.status_code == 200:
                return resp.json()["embeddings"][0]
            logger.warning(f"Embed request failed: {resp.status_code}")
            return None
        except Exception as e:
            logger.error(f"Embed error: {e}")
            return None

    async def add_document(
        self,
        collection_base: str,
        doc_id: str,
        text: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Upsert a document + embedding into postgres."""
        if not self._initialized or not self.circuit_breaker.can_execute():
            return False

        embedding = await self._embed(text)
        if embedding is None:
            return False

        meta_json = json.dumps(metadata or {})
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO embeddings (id, source_type, source_id, content, embedding, model, metadata)
                VALUES (%s, %s, %s, %s, %s::vector, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    content    = EXCLUDED.content,
                    embedding  = EXCLUDED.embedding,
                    metadata   = EXCLUDED.metadata,
                    created_at = NOW()
            """, (
                doc_id, collection_base, doc_id, text[:8000],
                str(embedding), self.current_model, meta_json
            ))
            conn.commit()
            conn.close()
            self.circuit_breaker.record_success()
            return True
        except Exception as e:
            logger.error(f"VectorStore add_document failed for {doc_id}: {e}")
            self.circuit_breaker.record_failure()
            return False

    async def search_similar(
        self,
        query: str,
        collection_base: str = "atoms",
        limit: int = 10,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Cosine similarity search in postgres embeddings table."""
        if not self._initialized or not self.circuit_breaker.can_execute():
            return []

        query_vec = await self._embed(query)
        if query_vec is None:
            return []

        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Build optional metadata filter
            # where = {"key": "value"} -> metadata @> '{"key": "value"}'
            meta_filter = ""
            params: list = [str(query_vec), collection_base, limit]
            if where:
                meta_filter = "AND metadata @> %s::jsonb"
                params.insert(2, json.dumps(where))  # before LIMIT
                # Reorder: vec, source_type, meta, limit
                params = [str(query_vec), collection_base, json.dumps(where), limit]

            sql = f"""
                SELECT id, source_type, source_id, content, metadata,
                       1 - (embedding <=> %s::vector) AS similarity
                FROM embeddings
                WHERE source_type = %s
                {meta_filter}
                ORDER BY embedding <=> %s::vector
                LIMIT %s
            """
            # Add second copy of query_vec for ORDER BY
            if where:
                cur.execute(sql, (str(query_vec), collection_base, json.dumps(where), str(query_vec), limit))
            else:
                cur.execute(sql, (str(query_vec), collection_base, str(query_vec), limit))

            rows = cur.fetchall()
            conn.close()
            self.circuit_breaker.record_success()

            results = []
            for row in rows:
                results.append({
                    "id":       row["id"],
                    "distance": 1.0 - float(row["similarity"]),  # ChromaDB returns distance (lower=better)
                    "document": row["content"],
                    "metadata": row["metadata"] if isinstance(row["metadata"], dict)
                                else json.loads(row["metadata"] or "{}")
                })
            return results

        except Exception as e:
            logger.error(f"VectorStore search_similar failed: {e}")
            self.circuit_breaker.record_failure()
            return []

    async def health_check(self) -> bool:
        if not self.circuit_breaker.can_execute():
            return False
        try:
            client = await self._get_client()
            resp = await client.get("/health")
            if resp.status_code == 200:
                self.circuit_breaker.record_success()
                return True
            self.circuit_breaker.record_failure()
            return False
        except Exception:
            self.circuit_breaker.record_failure()
            return False

    # Compat: membrane.py calls chromadb.upsert_atom
    async def upsert_atom(self, atom_id: str, text: str, metadata: Optional[Dict] = None) -> bool:
        return await self.add_document("atoms", atom_id, text, metadata)


# Global singleton
_vector_store = VectorStore()


def get_vector_store() -> VectorStore:
    return _vector_store
