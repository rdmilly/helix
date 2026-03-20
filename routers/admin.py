"""Admin Router - Maintenance and re-indexing operations.

Provides endpoints for re-embedding ChromaDB documents after
embedding model changes or initial deployment fixes.
"""
import json
from services import pg_sync
import logging
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from services.database import get_db
from services.chromadb import get_chromadb_service
from services.embeddings import get_embedding_service
from services.meta import get_meta_service

logger = logging.getLogger(__name__)

admin_router = APIRouter(prefix="/api/v1/admin")


@admin_router.post("/reindex")
async def reindex_chromadb():
    """Re-embed and re-index all documents in ChromaDB.
    
    Reads atoms and sessions from SQLite, computes fresh embeddings
    via the current model, and upserts into ChromaDB collections.
    
    Use after: embedding model change, ChromaDB data loss,
    or fixing missing embeddings from pre-fastembed deployment.
    """
    embedder = get_embedding_service()
    if not embedder.is_ready:
        return JSONResponse(status_code=503, content={
            "error": "Embedding model not ready"
        })
    
    chromadb = get_chromadb_service()
    if not chromadb._initialized:
        return JSONResponse(status_code=503, content={
            "error": "ChromaDB not initialized"
        })
    
    db = get_db()
    meta = get_meta_service()
    stats = {"atoms_reindexed": 0, "sessions_reindexed": 0, "entities_reindexed": 0, "errors": []}
    
    # 1. Re-index atoms
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, name, meta FROM atoms")
            atoms = cursor.fetchall()
        
        for atom_id, name, meta_json in atoms:
            try:
                atom_meta = pg_sync.dejson(meta_json) if meta_json else {}
                semantic = atom_meta.get("semantic", {})
                structural = atom_meta.get("structural", {})
                category = semantic.get("category", "general") if isinstance(semantic, dict) else "general"
                tags = semantic.get("semantic_tags", []) if isinstance(semantic, dict) else []
                
                text = f"{name}: {category} function. Tags: {', '.join(tags) if tags else 'general'}"
                
                success = await chromadb.add_document(
                    collection_base="atoms",
                    doc_id=atom_id,
                    text=text,
                    metadata={
                        "name": name,
                        "category": category,
                        "line_count": structural.get("line_count", 0) if isinstance(structural, dict) else 0,
                        "structural_fp": atom_meta.get("structural_fp", ""),
                    }
                )
                if success:
                    stats["atoms_reindexed"] += 1
                else:
                    stats["errors"].append(f"atom:{atom_id} upsert failed")
            except Exception as e:
                stats["errors"].append(f"atom:{atom_id} error: {str(e)[:100]}")
    except Exception as e:
        stats["errors"].append(f"atoms query failed: {str(e)[:100]}")
    
    # 2. Re-index sessions (use summary from meta if available)
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, meta FROM sessions")
            sessions = cursor.fetchall()
        
        for session_id, meta_json in sessions:
            try:
                # Try to get summary from meta system
                session_meta = {}
                try:
                    session_meta = meta.read_meta("sessions", session_id)
                except (ValueError, KeyError):
                    pass
                
                # Look for summary in intake or analysis meta
                summary = ""
                if isinstance(session_meta, dict):
                    intake = session_meta.get("intake", {})
                    analysis = session_meta.get("analysis", {})
                    if isinstance(analysis, dict):
                        summary = analysis.get("summary", "")
                    if not summary and isinstance(intake, dict):
                        summary = intake.get("content", "")[:500]
                
                if not summary:
                    # Fallback: use session meta JSON
                    summary = f"Session {session_id}"
                
                success = await chromadb.add_document(
                    collection_base="sessions",
                    doc_id=session_id,
                    text=summary,
                    metadata={
                        "session_id": session_id,
                        "has_summary": bool(summary and summary != f"Session {session_id}"),
                    }
                )
                if success:
                    stats["sessions_reindexed"] += 1
                else:
                    stats["errors"].append(f"session:{session_id} upsert failed")
            except Exception as e:
                stats["errors"].append(f"session:{session_id} error: {str(e)[:100]}")
    except Exception as e:
        stats["errors"].append(f"sessions query failed: {str(e)[:100]}")
    
    # 3. Re-index entities from session meta
    entity_count = 0
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM sessions")
            session_ids = [row[0] for row in cursor.fetchall()]
        
        for session_id in session_ids:
            try:
                entity_meta = meta.read_meta("sessions", session_id, "entities")
                if entity_meta and isinstance(entity_meta, dict):
                    # Build entity text from all entity types
                    parts = []
                    for entity_type in ("people", "projects", "services", "technologies"):
                        items = entity_meta.get(entity_type, [])
                        if items:
                            parts.append(f"{entity_type}: {', '.join(items)}")
                    
                    if parts:
                        entity_text = f"Entities from session {session_id}: {'; '.join(parts)}"
                        success = await chromadb.add_document(
                            collection_base="entities",
                            doc_id=f"entities_{session_id}",
                            text=entity_text,
                            metadata={
                                "session_id": session_id,
                                "people": json.dumps(entity_meta.get("people", [])),
                                "projects": json.dumps(entity_meta.get("projects", [])),
                                "services": json.dumps(entity_meta.get("services", [])),
                                "technologies": json.dumps(entity_meta.get("technologies", [])),
                            }
                        )
                        if success:
                            entity_count += 1
            except (ValueError, KeyError):
                pass
    except Exception as e:
        stats["errors"].append(f"entities query failed: {str(e)[:100]}")
    
    stats["entities_reindexed"] = entity_count
    
    logger.info(
        f"Reindex complete: {stats['atoms_reindexed']} atoms, "
        f"{stats['sessions_reindexed']} sessions, "
        f"{stats['entities_reindexed']} entities, "
        f"{len(stats['errors'])} errors"
    )
    
    return {
        "status": "complete",
        "model": embedder.model_name,
        "stats": stats,
    }
