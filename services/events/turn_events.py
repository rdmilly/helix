"""Turn Event Handlers — Subscribers for turn.flush

Fired by MemBrain after each stream_complete (Phase 1.3).
Full-turn fan-out: all 4 processors run concurrently on every turn.

Subscribers:
  - code_scanner    : Extract code blocks from messages -> atoms
  - text_processor  : Summarize + extract entities/decisions (Haiku)
  - action_parser   : Parse tool calls, file ops, commands
  - kg_extractor    : Extract and upsert entities into KG
"""
import asyncio
import logging
import re
from typing import Any, Dict, List, Tuple

log = logging.getLogger("helix.events.turn")

CODE_FENCE_RE = re.compile(r'```(\w*)\s*\n(.*?)```', re.DOTALL)
CODE_LANGUAGES = {
    "python", "py", "javascript", "js", "typescript", "ts",
    "rust", "go", "java", "c", "cpp", "ruby", "bash", "sh",
    "sql", "html", "css", "yaml", "toml", "json", "dockerfile",
}


def _extract_code_blocks(messages: List[Dict]) -> List[Tuple[str, str]]:
    """Extract (lang, code) tuples from assistant messages."""
    blocks = []
    for msg in messages:
        if msg.get("role") != "assistant":
            continue
        content = msg.get("content", "")
        if isinstance(content, list):
            content = " ".join(
                p.get("text", "") for p in content
                if isinstance(p, dict) and p.get("type") == "text"
            )
        for match in CODE_FENCE_RE.finditer(content):
            lang = match.group(1).lower().strip() or "python"
            if lang not in CODE_LANGUAGES:
                lang = "python"
            code = match.group(2).strip()
            if len(code) > 20:
                blocks.append((lang, code))
    return blocks


def _extract_text(messages: List[Dict]) -> str:
    """Concatenate all text content from messages for KG analysis."""
    parts = []
    for msg in messages:
        content = msg.get("content", "")
        if isinstance(content, str) and content.strip():
            parts.append(content[:2000])
        elif isinstance(content, list):
            for p in content:
                if isinstance(p, dict) and p.get("type") == "text":
                    parts.append(p.get("text", "")[:1000])
    return "\n".join(parts)[:6000]


async def handle_turn_flush(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Fan-out full turn to all 4 processors concurrently.

    Payload shape (from MemBrain per-turn flush, Phase 1.3):
      session_id  : str
      turn_index  : int
      messages    : list of {role, content} dicts for this turn
      provider    : str   (optional)
      model       : str   (optional)
    """
    session_id = payload.get("session_id", "")
    messages = payload.get("messages", [])
    turn_index = payload.get("turn_index", 0)

    log.info(
        f"turn.flush fan-out: session={session_id} "
        f"turn={turn_index} msgs={len(messages)}"
    )

    tasks = [
        _scan_code(messages, session_id),
        _process_text(messages, session_id, payload),
        _parse_actions(payload, session_id),
        _extract_kg(messages, session_id),
    ]
    labels = ["code_scanner", "text_processor", "action_parser", "kg_extractor"]

    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results = {}
    for label, result in zip(labels, raw_results):
        if isinstance(result, Exception):
            log.warning(f"turn.flush subscriber '{label}' failed: {result}")
            results[label] = {"status": "error", "error": str(result)}
        else:
            results[label] = result if isinstance(result, dict) else {"status": "ok"}

    return {
        "event": "turn.flush",
        "session_id": session_id,
        "turn_index": turn_index,
        "results": results,
    }


# ============================================================
# Subscriber implementations
# ============================================================

async def _scan_code(messages: List[Dict], session_id: str) -> Dict[str, Any]:
    """Extract code blocks from assistant messages and create/update atoms."""
    try:
        from services.scanner import get_scanner_service
        scanner = get_scanner_service()
        blocks = _extract_code_blocks(messages)
        if not blocks:
            return {"status": "skipped", "reason": "no_code_blocks"}

        all_atoms = []
        for lang, code in blocks:
            atoms = await scanner.extract_atoms(
                code,
                language=lang,
                filepath=f"<turn:{session_id}:{lang}>",
            )
            all_atoms.extend(atoms)

        created = sum(1 for a in all_atoms if a.get("action") == "created")
        updated = sum(1 for a in all_atoms if a.get("action") == "updated")
        log.info(
            f"turn.flush code_scanner: {len(blocks)} blocks, "
            f"{created} atoms created, {updated} updated"
        )
        return {
            "status": "ok",
            "blocks": len(blocks),
            "atoms_created": created,
            "atoms_updated": updated,
        }
    except Exception as e:
        log.debug(f"Code scan failed: {e}")
        return {"status": "error", "error": str(e)}


async def _process_text(
    messages: List[Dict], session_id: str, payload: Dict[str, Any]
) -> Dict[str, Any]:
    """Summarize turn + extract entities and decisions via Haiku."""
    try:
        from services.haiku import get_haiku_service
        from services.chromadb import get_chromadb_service
        from services.meta import get_meta_service

        haiku = get_haiku_service()
        chromadb = get_chromadb_service()
        meta = get_meta_service()
        turn_index = payload.get("turn_index", 0)

        summary = await haiku.summarize_session(messages)
        entities = await haiku.extract_entities(summary)
        decisions = await haiku.extract_decisions(summary)

        meta.write_meta("sessions", session_id, f"turn_{turn_index}_analysis", {
            "summary": summary,
            "turn_index": turn_index,
            "entities": entities,
            "decision_count": len(decisions) if decisions else 0,
        }, written_by="turn_events.text_processor_v1")

        if summary and summary != "Summary unavailable":
            turn_doc_id = f"{session_id}_t{turn_index}"
            await chromadb.add_document(
                collection_base="sessions",
                doc_id=turn_doc_id,
                text=summary,
                metadata={"session_id": session_id, "turn_index": turn_index},
            )

        return {
            "status": "ok",
            "summary_len": len(summary),
            "has_entities": bool(entities),
            "decisions": len(decisions or []),
        }
    except Exception as e:
        log.debug(f"Text processing failed: {e}")
        return {"status": "error", "error": str(e)}


async def _parse_actions(payload: Dict[str, Any], session_id: str) -> Dict[str, Any]:
    """Parse tool calls and file ops from the turn payload."""
    try:
        from services.parser import get_parser_service
        from services.meta import get_meta_service

        parser = get_parser_service()
        meta = get_meta_service()
        turn_index = payload.get("turn_index", 0)

        result = await parser.parse_actions(payload, session_id)

        if session_id and result.get("entities_found"):
            meta.write_meta("sessions", session_id, f"turn_{turn_index}_actions", {
                "entities": result["entities_found"],
                "decisions": result.get("decisions_found", []),
            }, written_by="turn_events.action_parser_v1")

        return {
            "status": "ok",
            "entities_found": result.get("entities_found", 0),
            "decisions_found": result.get("decisions_found", 0),
        }
    except Exception as e:
        log.debug(f"Action parsing failed: {e}")
        return {"status": "error", "error": str(e)}


async def _extract_kg(messages: List[Dict], session_id: str) -> Dict[str, Any]:
    """Extract entities from turn text and upsert into knowledge graph."""
    try:
        from services.workbench import get_workbench
        wb = get_workbench()
        text = _extract_text(messages)
        if not text.strip():
            return {"status": "skipped", "reason": "no_text"}
        result = await asyncio.to_thread(
            wb.extract_entities, text, f"turn:{session_id}", session_id
        )
        return result if isinstance(result, dict) else {"status": "ok"}
    except Exception as e:
        log.debug(f"KG extraction failed: {e}")
        return {"status": "error", "error": str(e)}
