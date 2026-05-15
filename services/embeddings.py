"""Embedding Service — routes to helix-embeddings sidecar via HTTP.

Consolidated architecture: all embeddings go through one service.
helix-embeddings runs BAAI/bge-large-en-v1.5 (1024 dims) via ONNX.

Previous: helix-cortex loaded its own ONNX model (1.3GB in memory).
Now: HTTP call to helix-embeddings sidecar (already running, shared).

Benefits:
  - helix-cortex uses ~1.3GB less memory
  - No startup delay loading ONNX
  - One consistent embedding service: ChromaDB + pgvector + scanner all identical
  - ONNX runtime on sidecar is already optimized and warm
"""
import logging
import os
from typing import List, Optional

logger = logging.getLogger(__name__)

EMBEDDINGS_URL = os.getenv("EMBEDDINGS_URL", "http://helix-embeddings:8000")


class EmbeddingService:
    """Proxy to helix-embeddings sidecar. Same interface as the old fastembed class.
    All embedding calls go to the sidecar via HTTP — one model, one service.
    """

    def __init__(self):
        self._ready = False
        self._model_name = "BAAI/bge-large-en-v1.5"
        self._url = EMBEDDINGS_URL

    async def initialize(self) -> bool:
        """Verify helix-embeddings sidecar is reachable."""
        import httpx
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.get(f"{self._url}/health")
                if resp.status_code == 200:
                    data = resp.json()
                    self._model_name = data.get("model", self._model_name)
                    self._ready = True
                    logger.info(f"EmbeddingService ready: {self._model_name} via {self._url}")
                    return True
        except Exception as e:
            logger.warning(f"EmbeddingService: helix-embeddings unreachable: {e}")
        self._ready = False
        return False

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """Compute embeddings via sidecar. Synchronous wrapper."""
        if not texts:
            return []
        import httpx
        try:
            resp = httpx.post(
                f"{self._url}/embed",
                json={"texts": texts},
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.json().get("embeddings", [])
        except Exception as e:
            logger.error(f"embed_texts failed: {e}")
        return []

    async def embed_texts_async(self, texts: List[str]) -> List[List[float]]:
        """Async version for use in async contexts."""
        if not texts:
            return []
        import httpx
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{self._url}/embed",
                    json={"texts": texts},
                )
                if resp.status_code == 200:
                    return resp.json().get("embeddings", [])
        except Exception as e:
            logger.error(f"embed_texts_async failed: {e}")
        return []

    def embed_single(self, text: str) -> Optional[List[float]]:
        results = self.embed_texts([text])
        return results[0] if results else None

    @property
    def is_ready(self) -> bool:
        return self._ready

    @property
    def model_name(self) -> str:
        return self._model_name


_embedding_service = EmbeddingService()


def get_embedding_service() -> EmbeddingService:
    return _embedding_service
