"""Session Event Handlers — Subscribers for session.ingested

Fired after ext_ingest flushes a conversation batch to storage.

Subscribers:
  - haiku_summarizer  : Summarize + extract decisions via Haiku
  - chromadb_embedder : Embed summary into ChromaDB sessions collection
  - session_store     : Finalize session metadata
"""
import asyncio
import logging
from typing import Any, Dict

log = logging.getLogger("helix.events.session")


async def handle_session_ingested(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Dispatch all session.ingested subscribers concurrently."""
    session_id = payload.get("session_id", "")
    messages = payload.get("messages", [])
    provider = payload.get("provider", "unknown")

    log.info(f"session.ingested dispatching: {session_id} ({len(messages)} messages)")

    tasks = []
    labels = []

    tasks.append(_summarize_session(session_id, messages, provider))
    labels.append("haiku_summarizer")

    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results = {}
    for label, result in zip(labels, raw_results):
        if isinstance(result, Exception):
            log.warning(f"session.ingested subscriber '{label}' failed: {result}")
            results[label] = {"status": "error", "error": str(result)}
        else:
            results[label] = result if isinstance(result, dict) else {"status": "ok"}

    return {"event": "session.ingested", "session_id": session_id, "results": results}


async def _summarize_session(
    session_id: str, messages: list, provider: str
) -> Dict[str, Any]:
    """Summarize conversation and extract decisions via Haiku."""
    try:
        from services.haiku import get_haiku_service
        from services.chromadb import get_chromadb_service
        from services.meta import get_meta_service

        haiku = get_haiku_service()
        chromadb = get_chromadb_service()
        meta = get_meta_service()

        # Summarize
        summary = await haiku.summarize_session(messages)

        # Extract decisions
        decisions = await haiku.extract_decisions(summary)

        # Write to meta
        meta.write_meta("sessions", session_id, "analysis", {
            "summary": summary,
            "decision_count": len(decisions) if decisions else 0,
            "provider": provider,
        }, written_by="session_events.summarizer_v1")

        # Store in ChromaDB
        if summary and summary != "Summary unavailable":
            await chromadb.add_document(
                collection_base="sessions",
                doc_id=session_id,
                text=summary,
                metadata={"session_id": session_id, "provider": provider},
            )

        return {"status": "ok", "summary_len": len(summary), "decisions": len(decisions or [])}
    except Exception as e:
        log.warning(f"Session summarization failed: {e}")
        return {"status": "error", "error": str(e)}
