"""ChromaDB compatibility shim — Phase 4 migration.

All callers continue to `from services.chromadb import get_chromadb_service`
but now receive the pgvector-backed VectorStore under the hood.

Original implementation preserved at chromadb.py.bak-phase4.
"""
from services.vector_store import VectorStore, get_vector_store

# Alias so existing isinstance() checks keep working
ChromaDBService = VectorStore

# Module-level singleton
chromadb_service = get_vector_store()


def get_chromadb_service() -> VectorStore:
    """Get the vector store instance (was ChromaDB, now pgvector)."""
    return get_vector_store()
