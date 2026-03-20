"""Exchange Event Handlers — Subscribers for exchange.posted

Fired after helix_exchange_post is called (Say/Do/Think convergence point).

Subscribers:
  - archive_router : Route to decisions/failures/patterns/sessions collections
  - kg_extractor   : Extract entities from the exchange content
  - observer_log   : Record the exchange in activity log
"""
import asyncio
import logging
from typing import Any, Dict

log = logging.getLogger("helix.events.exchange")


async def handle_exchange_posted(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Dispatch all exchange.posted subscribers concurrently."""
    session_id = payload.get("session_id", "")
    exchange_type = payload.get("type", "observation")
    content = payload.get("content", "")
    project = payload.get("project", "")

    log.info(f"exchange.posted dispatching: {exchange_type} session={session_id}")

    tasks = []
    labels = []

    tasks.append(_route_to_archive(payload))
    labels.append("archive_router")

    tasks.append(_extract_kg_entities(content, session_id, project))
    labels.append("kg_extractor")

    tasks.append(_log_observer(session_id, exchange_type, content))
    labels.append("observer_log")

    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results = {}
    for label, result in zip(labels, raw_results):
        if isinstance(result, Exception):
            log.warning(f"exchange.posted subscriber '{label}' failed: {result}")
            results[label] = {"status": "error", "error": str(result)}
        else:
            results[label] = result if isinstance(result, dict) else {"status": "ok"}

    return {"event": "exchange.posted", "type": exchange_type, "results": results}


async def _route_to_archive(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Route exchange to appropriate archive collection."""
    try:
        from services.exchange import get_exchange_service
        svc = get_exchange_service()
        result = await asyncio.to_thread(svc.route_to_archive, payload)
        return result if isinstance(result, dict) else {"status": "ok"}
    except Exception as e:
        log.debug(f"Archive routing failed: {e}")
        return {"status": "skipped", "reason": str(e)}


async def _extract_kg_entities(
    content: str, session_id: str, project: str
) -> Dict[str, Any]:
    """Extract entities from exchange content into the KG."""
    try:
        from services.workbench import get_workbench
        wb = get_workbench()
        result = await asyncio.to_thread(
            wb.extract_entities, content, f"exchange:{session_id}", session_id
        )
        return result
    except Exception as e:
        log.debug(f"KG extraction from exchange failed: {e}")
        return {"status": "skipped", "reason": str(e)}


async def _log_observer(
    session_id: str, exchange_type: str, content: str
) -> Dict[str, Any]:
    """Record the exchange in observer activity log."""
    try:
        from services.workbench import get_workbench
        wb = get_workbench()
        result = await asyncio.to_thread(
            wb.log_activity, "exchange", "",
            {"type": exchange_type, "session_id": session_id, "content_len": len(content)},
            session_id
        )
        return result
    except Exception as e:
        log.debug(f"Observer log for exchange failed: {e}")
        return {"status": "skipped", "reason": str(e)}
