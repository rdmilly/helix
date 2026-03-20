"""Embedding Service - BGE-M3 via FastEmbed (ONNX)

Computes dense embeddings for ChromaDB storage and search.
Model downloads to persistent volume on first run, cached after.

BGE-M3 chosen for code semantic understanding -- critical for
distinguishing similar code patterns in the DNA library.
"""
import logging
from typing import List, Optional
from pathlib import Path

from fastembed import TextEmbedding

from config import CURRENT_EMBEDDING_MODEL, EMBEDDING_DIMENSIONS, DATA_DIR

logger = logging.getLogger(__name__)

# Cache model in persistent volume so it survives rebuilds
MODEL_CACHE_DIR = str(DATA_DIR / "models")

# Map our config names to fastembed model identifiers
MODEL_MAP = {
    "bge-m3": "BAAI/bge-m3",
    "bge-large-en-v1.5": "BAAI/bge-large-en-v1.5",
    "bge-base-en-v1.5": "BAAI/bge-base-en-v1.5",
    "bge-small-en-v1.5": "BAAI/bge-small-en-v1.5",
    "all-MiniLM-L6-v2": "sentence-transformers/all-MiniLM-L6-v2",
}


class EmbeddingService:
    """Compute text embeddings using FastEmbed (ONNX runtime).
    
    Thread-safe singleton. Model loads lazily on first embed call.
    Downloads ~600MB on first run (cached in persistent volume).
    """
    
    def __init__(self):
        self._model: Optional[TextEmbedding] = None
        self._model_name = MODEL_MAP.get(CURRENT_EMBEDDING_MODEL, CURRENT_EMBEDDING_MODEL)
        self._ready = False
    
    def initialize(self) -> bool:
        """Load the embedding model. Call during app startup."""
        if self._ready:
            return True
        
        try:
            Path(MODEL_CACHE_DIR).mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Loading embedding model: {self._model_name} (cache: {MODEL_CACHE_DIR})")
            self._model = TextEmbedding(
                model_name=self._model_name,
                cache_dir=MODEL_CACHE_DIR,
            )
            
            # Warm up with a test embed
            test = list(self._model.embed(["test"]))
            actual_dim = len(test[0])
            logger.info(f"Embedding model ready: {self._model_name} (dim={actual_dim})")
            
            if actual_dim != EMBEDDING_DIMENSIONS:
                logger.warning(
                    f"Dimension mismatch: config says {EMBEDDING_DIMENSIONS}, "
                    f"model produces {actual_dim}. Using actual: {actual_dim}"
                )
            
            self._ready = True
            return True
            
        except Exception as e:
            logger.error(f"Failed to load embedding model: {e}")
            self._ready = False
            return False
    
    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """Compute embeddings for a list of texts.
        
        Returns list of float vectors. Empty list on failure.
        """
        if not self._ready or not self._model:
            logger.warning("Embedding model not ready, returning empty")
            return []
        
        if not texts:
            return []
        
        try:
            # fastembed returns a generator of numpy arrays
            embeddings = list(self._model.embed(texts))
            return [emb.tolist() for emb in embeddings]
        except Exception as e:
            logger.error(f"Embedding computation failed: {e}")
            return []
    
    def embed_single(self, text: str) -> Optional[List[float]]:
        """Compute embedding for a single text. Returns None on failure."""
        results = self.embed_texts([text])
        return results[0] if results else None
    
    @property
    def is_ready(self) -> bool:
        return self._ready
    
    @property
    def model_name(self) -> str:
        return CURRENT_EMBEDDING_MODEL


# Global singleton
_embedding_service = EmbeddingService()


def get_embedding_service() -> EmbeddingService:
    """Get embedding service instance."""
    return _embedding_service
