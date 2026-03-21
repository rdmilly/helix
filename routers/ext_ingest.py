from services import pg_sync
"""
ext_ingest.py - MemBrain Extension Ingest Router

POST /api/v1/ext/ingest

Accepts structured turn arrays from the MemBrain Chrome extension,
groups by conversationId, formats into transcript text, and feeds
into the existing conversation RAG pipeline.

Intelligence extraction (9-tag taxonomy):
  DECISION    -> decisions table + structured_archive + ChromaDB
  ASSUMPTION  -> structured_archive + ChromaDB
  CONSTRAINT  -> exchanges.constraint_discovered + structured_archive
  INVARIANT   -> kg_relationships + structured_archive
  RISK        -> anomalies table + structured_archive
  TRADEOFF    -> structured_archive + ChromaDB
  COUPLING    -> kg_relationships + structured_archive
  REJECTED    -> exchanges.rejected_alternatives + structured_archive
  PATTERN     -> conventions table + structured_archive
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from collections import defaultdict

from fastapi import APIRouter, Header
from pydantic import BaseModel, Field

from services import conversation_store
from services.haiku import HaikuService
from services.database import get_db_path

logger = logging.getLogger(__name__)

_haiku: Optional[HaikuService] = None

def _get_haiku() -> HaikuService:
    global _haiku
    if _haiku is None:
        from services.haiku import HaikuService
        _haiku = HaikuService()
    return _haiku

router = APIRouter(prefix="/api/v1/ext", tags=["Extension"])


class TurnPayload(BaseModel):
    id: Optional[str] = None
    platform: str = "unknown"
    conversationId: str = ""
    role: str = "assistant"
    content: str = ""
    captureType: Optional[str] = "unknown"
    url: Optional[str] = None
    timestamp: Optional[Any] = None


class ExtIngestPayload(BaseModel):
    turns: List[TurnPayload] = Field(..., description="Captured conversation turns")
    extensionVersion: Optional[str] = None
    flushedAt: Optional[str] = None


class ExtIngestResponse(BaseModel):
    success: int = 0
    conversations: int = 0
    turns_received: int = 0
    turns_ingested: int = 0
    errors: List[str] = []


ROLE_PREFIX = {
    "user": "Human",
    "assistant": "Assistant",
    "system": "System",
}

PLATFORM_SOURCE = {
    "claude": "claude-ai",
    "chatgpt": "chatgpt",
    "gemini": "gemini",
    "perplexity": "perplexity",
}


def _parse_timestamp(ts: Any) -> str:
    if ts is None:
        return datetime.now(timezone.utc).isoformat()
    if isinstance(ts, (int, float)):
        if ts > 1e11:
            ts = ts / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    return str(ts)


def _turns_to_transcript(turns: List[TurnPayload]) -> str:
    lines = []
    for t in turns:
        prefix = ROLE_PREFIX.get(t.role.lower(), t.role.capitalize())
        lines.append(f"{prefix}: {t.content.strip()}")
    return "\n\n".join(lines)


@router.post("/ingest", response_model=ExtIngestResponse)
async def ext_ingest(
    payload: ExtIngestPayload,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    turns = payload.turns
    if not turns:
        return ExtIngestResponse(turns_received=0, conversations=0)

    grouped: Dict[str, List[TurnPayload]] = defaultdict(list)
    for t in turns:
        key = t.conversationId or f"ext-{t.platform}-unknown"
        grouped[key].append(t)

    errors = []
    success_count = 0
    total_ingested = 0

    for conv_id, conv_turns in grouped.items():
        try:
            conv_turns.sort(key=lambda t: float(t.timestamp) if isinstance(t.timestamp, (int, float)) else 0.0)

            transcript = _turns_to_transcript(conv_turns)
            if not transcript.strip():
                continue

            platform = conv_turns[0].platform if conv_turns else "unknown"
            source = PLATFORM_SOURCE.get(platform, f"ext-{platform}")
            timestamp = _parse_timestamp(conv_turns[0].timestamp)

            from services.content_detector import detect
            ctype, cconf = detect(transcript)
            metadata = {
                "extension_version": payload.extensionVersion,
                "flushed_at": payload.flushedAt,
                "platform": platform,
                "turn_count": len(conv_turns),
                "capture_types": list({t.captureType for t in conv_turns if t.captureType}),
                "url": conv_turns[0].url,
                "content_type": ctype,
                "content_confidence": round(cconf, 2),
            }

            result = await conversation_store.ingest_conversation(
                text=transcript,
                session_id=conv_id,
                source=source,
                timestamp=timestamp,
                metadata=metadata,
            )

            success_count += 1
            total_ingested += len(conv_turns)
            logger.info(f"[ext_ingest] Ingested conv {conv_id} ({len(conv_turns)} turns, platform={platform})")

        except Exception as e:
            logger.error(f"[ext_ingest] Failed conv {conv_id}: {e}", exc_info=True)
            errors.append(f"{conv_id}: {str(e)}")

    # Async: summarize + extract intelligence for each ingested conversation
    if success_count > 0:
        import asyncio
        asyncio.create_task(_summarize_conversations(grouped, payload))

    return ExtIngestResponse(
        success=success_count,
        conversations=len(grouped),
        turns_received=len(turns),
        turns_ingested=total_ingested,
        errors=errors,
    )


async def _summarize_conversations(
    grouped: dict,
    payload: "ExtIngestPayload",
) -> None:
    """Background task: Haiku summarizes + extracts 9-tag intelligence for each conversation."""
    import sqlite3
    import uuid as uuid_mod
    haiku = _get_haiku()

    for conv_id, conv_turns in grouped.items():
        try:
            messages = [
                {"role": t.role, "content": t.content}
                for t in conv_turns if t.content and len(t.content) > 5
            ]
            if len(messages) < 2:
                continue

            summary = await haiku.summarize_session(messages)
            if not summary or summary == "Summary unavailable":
                continue

            full_text = " ".join(m["content"] for m in messages[:30])
            entities = await haiku.extract_entities(full_text)
            intelligence_items = await haiku.extract_intelligence(full_text)

            platform = conv_turns[0].platform if conv_turns else "claude"
            db_path = get_db_path()
            now = datetime.now(timezone.utc).isoformat()

            # --- Group intelligence items by tag for routing ---
            by_tag: Dict[str, List[Dict]] = defaultdict(list)
            for item in intelligence_items:
                tag = item.get("tag", "")
                if tag:
                    by_tag[tag].append(item)

            with pg_sync.sqlite_conn(db_path) as conn:

                # 1. Session summary -> structured_archive
                conn.execute(
                    "INSERT INTO structured_archive "
                    "(id, collection, content, metadata_json, session_id, timestamp, created_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (
                        f"ext-{conv_id}", "sessions", summary,
                        json.dumps({"source": f"ext-{platform}", "platform": platform,
                                    "turn_count": len(conv_turns), "ext_version": payload.extensionVersion}),
                        conv_id, now, now,
                    )
                )
                try:
                    row = conn.execute("SELECT rowid FROM structured_archive WHERE id=?",
                                       (f"ext-{conv_id}",)).fetchone()
                    if row:
                        conn.execute("INSERT INTO structured_fts(rowid,content,collection) VALUES(?,?,?)",
                                     (row[0], summary, "sessions"))
                except Exception:
                    pass

                # 2. All intelligence items -> structured_archive (intelligence collection)
                for item in intelligence_items[:25]:
                    tag = item.get("tag", "")
                    content = item.get("content", "")
                    if not tag or not content:
                        continue
                    item_id = str(uuid_mod.uuid4())[:12]
                    item_meta = json.dumps({
                        "tag": tag,
                        "component": item.get("component"),
                        "context": item.get("context", ""),
                        "confidence": item.get("confidence", 0.5),
                    })
                    conn.execute(
                        "INSERT INTO structured_archive "
                        "(id, collection, content, metadata_json, session_id, timestamp, created_at) "
                        "VALUES (?,?,?,?,?,?,?)",
                        (item_id, "intelligence", content, item_meta, conv_id, now, now)
                    )
                    try:
                        row = conn.execute("SELECT rowid FROM structured_archive WHERE id=?",
                                           (item_id,)).fetchone()
                        if row:
                            conn.execute(
                                "INSERT INTO structured_fts(rowid,content,collection) VALUES(?,?,?)",
                                (row[0], f"[{tag}] {content}", "intelligence")
                            )
                    except Exception:
                        pass

                # 3. DECISION -> decisions table
                for item in by_tag.get("DECISION", []):
                    content = item.get("content", "")
                    if not content:
                        continue
                    conn.execute(
                        "INSERT INTO decisions (id, session_id, decision, rationale, project, created_at, meta) "
                        "VALUES (?,?,?,?,?,?,?)",
                        (
                            str(uuid_mod.uuid4())[:12], conv_id, content,
                            item.get("context", ""),
                            item.get("component") or platform,
                            now,
                            json.dumps({"confidence": item.get("confidence", 0.7), "source": "intelligence"})
                        )
                    )

                # 4. RISK -> anomalies table
                for item in by_tag.get("RISK", []):
                    content = item.get("content", "")
                    if not content:
                        continue
                    confidence = float(item.get("confidence", 0.7))
                    severity = "high" if confidence >= 0.85 else "medium" if confidence >= 0.7 else "low"
                    conn.execute(
                        "INSERT INTO anomalies (id, type, description, evidence, severity, state, "
                        "session_id, created_at, meta) VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            str(uuid_mod.uuid4())[:12], "risk", content,
                            item.get("context", ""), severity, "open",
                            conv_id, now,
                            json.dumps({"component": item.get("component"), "confidence": confidence})
                        )
                    )

                # 5. PATTERN -> conventions table
                for item in by_tag.get("PATTERN", []):
                    content = item.get("content", "")
                    if not content:
                        continue
                    existing = conn.execute(
                        "SELECT id, occurrences FROM conventions WHERE pattern=?", (content,)
                    ).fetchone()
                    if existing:
                        conn.execute(
                            "UPDATE conventions SET occurrences=?, meta=? WHERE id=?",
                            (existing[1] + 1,
                             json.dumps({"last_session": conv_id}),
                             existing[0])
                        )
                    else:
                        conn.execute(
                            "INSERT INTO conventions (id, pattern, description, confidence, occurrences, "
                            "scope, first_seen, meta) VALUES (?,?,?,?,?,?,?,?)",
                            (
                                str(uuid_mod.uuid4())[:12], content,
                                item.get("context", ""),
                                float(item.get("confidence", 0.7)), 1,
                                item.get("component") or "general",
                                now,
                                json.dumps({"session_id": conv_id})
                            )
                        )

                # 6. COUPLING + INVARIANT -> kg_relationships
                for tag in ("COUPLING", "INVARIANT"):
                    for item in by_tag.get(tag, []):
                        content = item.get("content", "")
                        component = item.get("component") or ""
                        if not content:
                            continue
                        # Parse source/target: component is source, extract target from content if possible
                        source_name = component if component else "unknown"
                        target_name = content[:80]
                        conn.execute(
                            "INSERT INTO kg_relationships "
                            "(source_name, target_name, relation_type, description, created_at, session_id) "
                            "VALUES (?,?,?,?,?,?)",
                            (source_name, target_name, tag, content, now, conv_id)
                        )

                # 7. REJECTED + CONSTRAINT -> exchanges columns (update most recent exchange for this session)
                rejected_items = [i.get("content", "") for i in by_tag.get("REJECTED", []) if i.get("content")]
                constraint_items = [i.get("content", "") for i in by_tag.get("CONSTRAINT", []) if i.get("content")]
                pattern_items = [i.get("content", "") for i in by_tag.get("PATTERN", []) if i.get("content")]
                next_steps = [i.get("content", "") for i in by_tag.get("DECISION", []) if i.get("content")]

                if any([rejected_items, constraint_items, pattern_items, next_steps]):
                    # Find most recent exchange for this session or closest matching session
                    ex_row = conn.execute(
                        "SELECT id FROM exchanges WHERE session_id=? ORDER BY created_at DESC LIMIT 1",
                        (conv_id,)
                    ).fetchone()
                    if not ex_row:
                        # Try matching by proximity - most recent exchange overall
                        ex_row = conn.execute(
                            "SELECT id FROM exchanges ORDER BY created_at DESC LIMIT 1"
                        ).fetchone()
                    if ex_row:
                        conn.execute(
                            "UPDATE exchanges SET "
                            "rejected_alternatives=COALESCE(NULLIF(rejected_alternatives,''), ?), "
                            "constraint_discovered=COALESCE(NULLIF(constraint_discovered,''), ?), "
                            "pattern=COALESCE(NULLIF(pattern,''), ?), "
                            "next_step=COALESCE(NULLIF(next_step,''), ?) "
                            "WHERE id=?",
                            (
                                " | ".join(rejected_items[:3]) or None,
                                " | ".join(constraint_items[:3]) or None,
                                " | ".join(pattern_items[:2]) or None,
                                next_steps[0] if next_steps else None,
                                ex_row[0]
                            )
                        )

                conn.commit()

            # Nudges: create nudge for RISK items without a recent mitigating DECISION
            for item in by_tag.get('RISK', []):
                content = item.get('content', '')
                if not content:
                    continue
                try:
                    # Check if any recent DECISION addresses this risk (simple keyword overlap)
                    risk_words = set(content.lower().split()[:8])
                    mitigated = False
                    for dec in by_tag.get('DECISION', []):
                        dec_words = set(dec.get('content', '').lower().split())
                        if len(risk_words & dec_words) >= 2:
                            mitigated = True
                            break
                    if not mitigated:
                        conn.execute(
                            "INSERT INTO nudges (id, description, category, priority, state, "
                            "session_id, created_at, meta) VALUES (?,?,?,?,?,?,?,?)",
                            (
                                str(uuid_mod.uuid4())[:12],
                                f"[RISK] {content[:200]}",
                                "risk",
                                "medium",
                                "open",
                                conv_id,
                                now,
                                json.dumps({"component": item.get("component"), "confidence": item.get("confidence", 0.7)})
                            )
                        )
                except Exception:
                    pass

            # Nudges: create nudge for ASSUMPTION items older than 3 sessions without validation
            for item in by_tag.get('ASSUMPTION', []):
                content = item.get('content', '')
                if not content:
                    continue
                try:
                    conn.execute(
                        "INSERT INTO nudges (id, description, category, priority, state, "
                        "session_id, created_at, meta) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                        (
                            str(uuid_mod.uuid4())[:12],
                            f"[ASSUMPTION unvalidated] {content[:200]}",
                            "assumption",
                            "low",
                            "open",
                            conv_id,
                            now,
                            json.dumps({"component": item.get("component"), "needs_validation": True})
                        )
                    )
                except Exception:
                    pass

            conn.commit()

            # ChromaDB: session summary + each intelligence item as separate vector
            try:
                from services.chromadb import get_chromadb_service
                chroma = get_chromadb_service()
                await chroma.add_document(
                    collection_base="sessions",
                    doc_id=f"ext-{conv_id}",
                    text=summary,
                    metadata={
                        "session_id": conv_id,
                        "source": f"ext-{platform}",
                        "platform": platform,
                        "entities": str(entities),
                    }
                )
                for item in intelligence_items[:25]:
                    tag = item.get("tag", "")
                    content = item.get("content", "")
                    if not tag or not content:
                        continue
                    await chroma.add_document(
                        collection_base="intelligence",
                        doc_id=f"intel-{conv_id}-{tag}-{str(uuid_mod.uuid4())[:8]}",
                        text=f"[{tag}] {content}",
                        metadata={
                            "tag": tag,
                            "component": item.get("component") or "",
                            "context": item.get("context", ""),
                            "session_id": conv_id,
                            "platform": platform,
                        }
                    )
            except Exception as ce:
                logger.warning(f"[ext_ingest] ChromaDB index failed: {ce}")

            logger.info(
                f"[ext_ingest] Processed {conv_id}: {len(intelligence_items)} intelligence items "
                f"({len(by_tag)} tag types), entities: {list(entities.keys())}"
            )

            # Publish session.ingested — triggers summarizer + ChromaDB embedding via worker
            try:
                from services.event_bus import publish
                publish("session.ingested", {
                    "session_id": conv_id,
                    "provider": platform,
                    "intelligence_items": len(intelligence_items),
                    "tag_types": list(by_tag.keys()),
                    "messages": conv_turns[:50],  # cap to avoid payload bloat
                })
            except Exception as _be:
                logger.debug(f"[ext_ingest] session.ingested publish failed (non-fatal): {_be}")

        except Exception as e:
            logger.error(f"[ext_ingest] Summarization failed for {conv_id}: {e}", exc_info=True)


@router.get("/health")
async def ext_health():
    return {"status": "ok", "endpoint": "/api/v1/ext/ingest", "accepts": "turn_array"}
