from services import pg_sync
"""Observer Router — Unified Action Capture for Helix Cortex.

Receives tool call logs from the MCP provision-filter observer hook.
Also accepts file write events, KB changes, and document ingestion.
Everything flows into the Helix pipeline — one throat for all data.

Endpoints:
  POST /api/v1/observer/log       — Single action log (Memory-compatible)
  POST /api/v1/observer/log/batch — Batch action log (Memory-compatible)
  POST /api/v1/observer/exchange  — Full exchange (prompt + response)
  POST /api/v1/observer/webhook   — Universal webhook (auto-classifies)
  GET  /api/v1/observer/stats     — Observer statistics
  GET  /api/v1/observer/actions/recent — Recent actions
  GET  /api/v1/observer/facts     — Extracted infrastructure facts
"""

import json
import logging
import os
import re
import hashlib
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from services.database import get_db

# KB reindex on change
def _reindex_kb_file(content: str, file_path: str):
    """Auto-reindex KB file when changed via webhook."""
    try:
        import sqlite3, hashlib, json
        from pathlib import Path
        from services.database import get_db_path
        from datetime import datetime, timezone
        # Determine source from path
        source = None
        rel_path = file_path
        if "millyweb-kb" in file_path or "kb-gateway" in file_path:
            source = "infra-kb"
            for prefix in ["/opt/projects/millyweb-kb/", "millyweb-kb/"]:
                if file_path.startswith(prefix):
                    rel_path = file_path[len(prefix):]
                    break
        elif "working-kb" in file_path or "workdocs" in file_path:
            source = "working-kb"
            for prefix in ["/opt/data/working-kb/", "working-kb/", "/app/working-kb/"]:
                if file_path.startswith(prefix):
                    rel_path = file_path[len(prefix):]
                    break
        if not source or not content or not rel_path.endswith(".md"):
            return
        content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
        doc_id = f"{source}:{rel_path}"
        title = Path(rel_path).stem.replace("-", " ").replace("_", " ").title()
        for line in content.split("\n"):
            if line.startswith("# "):
                title = line[2:].strip()
                break
        conn = pg_sync.sqlite_conn(str(get_db_path()), timeout=10)
        conn.execute(
            "INSERT INTO kb_documents (id, source, path, title, content, content_hash, size_bytes, indexed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (doc_id, source, rel_path, title, content, content_hash, len(content.encode("utf-8")), datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
        conn.close()
        logger.info(f"KB auto-reindex: {doc_id}")
    except Exception as e:
        logger.warning(f"KB reindex failed for {file_path}: {e}")
from services.meta import get_meta_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/observer", tags=["Observer"])

SCAN_ENABLED = True

# File extension -> content type mapping
EXTENSION_MAP = {
    ".py": "code", ".js": "code", ".ts": "code", ".jsx": "code",
    ".tsx": "code", ".go": "code", ".rs": "code", ".rb": "code",
    ".java": "code", ".sh": "code", ".bash": "code",
    ".md": "document", ".txt": "document", ".rst": "document",
    ".yaml": "config", ".yml": "config", ".toml": "config",
    ".json": "config", ".env": "config", ".ini": "config",
    ".conf": "config",
}

# Patterns for infrastructure fact extraction
INFRA_PATTERNS = [
    (r'(?:port|PORT)\s*[=:]\s*(\d{2,5})', 'port_assignment'),
    (r'container_name:\s*(\S+)', 'container_name'),
    (r'(?:domain|DOMAIN|server_name)\s*[=:]\s*(\S+)', 'domain_assignment'),
    (r'(?:image|FROM)\s+(\S+:\S+)', 'docker_image'),
]

# Patterns for Python code fact extraction
CODE_PATTERNS = [
    (r"^(?:from|import)\s+([\w.]+)", "python_import"),
    (r"^class\s+(\w+)", "python_class"),
    (r"^(?:async\s+)?def\s+(\w+)", "python_function"),
    (r"@router\.(?:get|post|put|delete|patch)\(.[^)]*?([/][\w/{}]+)", "api_route"),
    (r"APIRouter\(prefix=.[^)]*?([/][\w/{}]+)", "api_prefix"),
    (r"os\.environ\.get\([\"']([A-Z_]{3,})", "env_var"),
]


# === Request Models ===

class ActionLog(BaseModel):
    timestamp: str = ""
    session_id: Optional[str] = None
    sequence_num: int = 0
    tool_name: str = ""
    server_name: Optional[str] = None
    category: str = "other"
    arguments: Dict[str, Any] = {}
    result_summary: Optional[str] = None
    duration_ms: Optional[int] = None
    error: bool = False
    has_file_content: bool = False
    file_path: Optional[str] = None
    file_content: Optional[str] = None


class BatchActionLog(BaseModel):
    actions: List[ActionLog]


class ExchangeLog(BaseModel):
    session_id: Optional[str] = None
    prompt: str = ""
    response: str = ""
    model: str = ""
    tool_calls: List[Dict[str, Any]] = []
    timestamp: str = ""


class WebhookPayload(BaseModel):
    """Universal webhook. Send anything, Helix classifies and routes."""
    source: str = Field("unknown", description="Source: provision-filter, workspace_write, millyext, kb, manual")
    content_type: Optional[str] = Field(None, description="Override: code, document, config, transcript, kb_change")
    content: str = Field("", description="Raw content")
    file_path: Optional[str] = Field(None, description="File path if file write")
    session_id: Optional[str] = Field(None)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    scan_code: bool = Field(True)
    update_kb: bool = Field(True)


# === Database Setup ===

def _ensure_tables():
    """No-op: tables exist in PostgreSQL (migration 001)."""
    pass
try:
    _ensure_tables()
    logger.info("Observer tables initialized in cortex.db")
except Exception as e:
    logger.warning(f"Observer table init deferred: {e}")


# === Helpers ===

def _classify_file(file_path, content=""):
    if not file_path:
        return "text"
    ext = os.path.splitext(file_path.lower())[1]
    if ext in EXTENSION_MAP:
        return EXTENSION_MAP[ext]
    if content:
        if "services:" in content[:200] or "docker-compose" in file_path.lower():
            return "infrastructure"
        if content[:50].startswith("FROM ") and "RUN " in content:
            return "infrastructure"
    return "document"


def _detect_language(file_path, content=""):
    if not file_path:
        return "unknown"
    m = {".py": "python", ".js": "javascript", ".ts": "typescript",
         ".go": "go", ".rs": "rust", ".sh": "bash", ".yaml": "yaml",
         ".yml": "yaml", ".json": "json", ".md": "markdown", ".html": "html"}
    ext = os.path.splitext(file_path.lower())[1]
    return m.get(ext, "unknown")


def _extract_facts(content, file_path=""):
    facts = []
    for pattern, fact_type in INFRA_PATTERNS:
        for match in re.finditer(pattern, content):
            val = match.group(1) if match.groups() else match.group(0)
            facts.append({"fact_type": fact_type, "fact_key": file_path,
                          "fact_value": val.strip().strip('"\'\''), "confidence": 0.9})
    for match in re.finditer(r'(\d{2,5}):(\d{2,5})', content):
        facts.append({"fact_type": "port_mapping", "fact_key": file_path,
                      "fact_value": f"{match.group(1)}:{match.group(2)}", "confidence": 0.8})
    return facts


def _extract_code_facts(content: str, file_path: str = "") -> list:
    """Extract facts from Python source files."""
    facts = []
    seen = set()
    for pattern, fact_type in CODE_PATTERNS:
        for match in re.finditer(pattern, content, re.MULTILINE):
            val = match.group(1).strip()
            key = f"{fact_type}:{val}"
            if val and key not in seen:
                seen.add(key)
                facts.append({"fact_type": fact_type, "fact_key": file_path,
                               "fact_value": val, "confidence": 0.95})
    return facts[:50]  # cap per file


def _store_facts(facts, source_file=""):
    if not facts:
        return 0
    db = get_db()
    stored = 0
    with db.get_connection() as conn:
        for f in facts:
            try:
                conn.execute("""
                    INSERT INTO observer_facts
                    (source_file, fact_type, fact_key, fact_value, confidence)
                    VALUES (?, ?, ?, ?, ?)
                """, (source_file, f["fact_type"], f["fact_key"], f["fact_value"], f.get("confidence", 1.0)))
                stored += 1
            except Exception:
                pass
        conn.commit()
    return stored


def _looks_like_transcript(content):
    markers = ["Human:", "Assistant:", "Claude:", "User:", "H:", "A:"]
    first_2k = content[:2000]
    return sum(1 for m in markers if m in first_2k) >= 2


def _has_code_blocks(content):
    return "```" in content


async def _index_document(content, file_path, content_type, session_id=""):
    try:
        from services.chromadb import get_chromadb_service
        chromadb = get_chromadb_service()
        doc_id = f"doc-{file_path.replace('/', '-').replace('.', '-')}" if file_path else f"doc-{hashlib.sha256(content[:200].encode()).hexdigest()[:12]}"
        await chromadb.add_document(
            collection_base="documents",
            doc_id=doc_id,
            text=content[:8000],
            metadata={"file_path": file_path, "content_type": content_type,
                      "char_count": len(content), "session_id": session_id,
                      "indexed_at": datetime.now(timezone.utc).isoformat()}
        )
        return True
    except Exception as e:
        logger.warning(f"Document index failed for {file_path}: {e}")
        return False


async def _ingest_transcript(content, session_id, source, metadata):
    try:
        from services import conversation_store
        if not session_id:
            h = hashlib.sha256(content[:500].encode()).hexdigest()[:12]
            session_id = f"conv-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{h}"
        result = await conversation_store.ingest_conversation(
            text=content, session_id=session_id, source=source,
            timestamp=datetime.now(timezone.utc).isoformat(), metadata=metadata)
        logger.info(f"Observer: transcript {session_id} -> {result.get('chunks', 0)} chunks")
    except Exception as e:
        logger.error(f"Transcript ingest failed: {e}", exc_info=True)


async def _process_file_capture(action_id, file_path, content, session_id):
    content_type = _classify_file(file_path, content)
    language = _detect_language(file_path, content)
    db = get_db()
    with db.get_connection() as conn:
        cursor = conn.execute("""
            INSERT INTO observer_file_captures
            (action_id, file_path, content, language, content_type, char_count)
            VALUES (?, ?, ?, ?, ?, ?) RETURNING id
        """, (action_id, file_path, content[:50000], language, content_type, len(content)))
        capture_id = cursor.lastrowid
        conn.commit()
    # Scan code
    if content_type == "code" and language == "python" and SCAN_ENABLED:
        try:
            from services.scanner import get_scanner_service
            import asyncio as _asyncio
            scanner = get_scanner_service()
            atoms = _asyncio.run(scanner.extract_atoms(content, language="python", filepath=file_path))
            if atoms:
                with db.get_connection() as conn:
                    conn.execute("UPDATE observer_file_captures SET scanned = 1 WHERE id = ?", (capture_id,))
                    conn.commit()
        except Exception as e:
            logger.warning(f"Code scan failed: {e}")
    # Index as document
    await _index_document(content, file_path, content_type, session_id)
    with db.get_connection() as conn:
        conn.execute("UPDATE observer_file_captures SET indexed = 1 WHERE id = ?", (capture_id,))
        conn.commit()
    # Extract facts from infra/config AND Python code files
    if content_type in ("config", "infrastructure"):
        facts = _extract_facts(content, file_path)
        if facts:
            stored = _store_facts(facts, file_path)
            with db.get_connection() as conn:
                conn.execute("UPDATE observer_file_captures SET facts_extracted = ? WHERE id = ?", (stored, capture_id))
                conn.commit()
            logger.info(f"Observer: {stored} infra facts from {file_path}")
    elif content_type == "code" and language == "python":
        facts = _extract_code_facts(content, file_path)
        if facts:
            stored = _store_facts(facts, file_path)
            with db.get_connection() as conn:
                conn.execute("UPDATE observer_file_captures SET facts_extracted = ? WHERE id = ?", (stored, capture_id))
                conn.commit()
            logger.info(f"Observer: {stored} code facts from {file_path}")

    # Shard diff: compare new content vs PREVIOUS captured version for this path.
    # Runs for ALL file captures (helix_file_write AND provisioner SSH writes).
    # Pass capture_id to exclude the just-inserted row from the "previous" lookup.
    try:
        from services.shard import get_shard_service
        shard = get_shard_service()
        diff_result = shard.diff_on_write(file_path, content, session_id or "", exclude_capture_id=capture_id)
        diff_status = diff_result.get("status", "unknown")
        if diff_status not in ("no_change", "pending_implementation"):
            added = diff_result.get("lines_added", 0)
            removed = diff_result.get("lines_removed", 0)
            logger.info(f"Observer shard diff: {file_path} +{added}/-{removed} ({diff_status})")
    except Exception as e:
        logger.debug(f"Shard diff skipped for {file_path}: {e}")


# === Routes ===


# ---------------------------------------------------------------------------
# Phase 7E: Sequence Detection
# ---------------------------------------------------------------------------
_session_buffers: dict = {}
SEQUENCE_WINDOW = 5

def _hash_sequence(tools: list) -> str:
    return hashlib.sha256('|'.join(tools).encode()).hexdigest()[:16]

def _track_sequence(session_id: str, tool_name: str):
    # Use "_global_" as key for null sessions — cross-session pattern detection
    sid = session_id if session_id else "_global_"
    if sid not in _session_buffers:
        _session_buffers[sid] = []
    _session_buffers[sid].append(tool_name)
    buf = _session_buffers[sid]
    if len(buf) > 100:
        _session_buffers[sid] = buf[-50:]
    if len(buf) >= 3:
        db = get_db()
        try:
            with db.get_connection() as conn:
                for window in range(3, min(len(buf) + 1, SEQUENCE_WINDOW + 1)):
                    seq = buf[-window:]
                    seq_hash = _hash_sequence(seq)
                    existing = conn.execute(
                        "SELECT id, occurrence_count FROM observer_sequences WHERE sequence_hash = ?",
                        (seq_hash,)
                    ).fetchone()
                    if existing:
                        conn.execute(
                            "UPDATE observer_sequences SET occurrence_count = occurrence_count + 1, last_seen = ? WHERE id = ?",
                            (datetime.now(timezone.utc).isoformat(), existing[0])
                        )
                    else:
                        conn.execute(
                            "INSERT INTO observer_sequences (session_id, tool_sequence, sequence_hash, length, first_seen, last_seen) VALUES (?, ?, ?, ?, ?, ?)",
                            (session_id, json.dumps(seq), seq_hash, len(seq),
                             datetime.now(timezone.utc).isoformat(),
                             datetime.now(timezone.utc).isoformat())
                        )
                conn.commit()
        except Exception as e:
            logger.warning(f"Sequence tracking error: {e}")

@router.post("/log")
async def log_action(action: ActionLog, background_tasks: BackgroundTasks):
    """Log a single tool call. Drop-in for Memory /api/observer/log."""
    db = get_db()
    with db.get_connection() as conn:
        cursor = conn.execute("""
            INSERT INTO observer_actions
            (timestamp, session_id, sequence_num, tool_name, server_name,
             category, arguments_json, result_summary, has_file_content,
             file_path, duration_ms, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id
        """, (
            action.timestamp or datetime.now(timezone.utc).isoformat(),
            action.session_id, action.sequence_num, action.tool_name,
            action.server_name, action.category,
            json.dumps(action.arguments)[:10000], action.result_summary,
            1 if action.has_file_content else 0, action.file_path,
            action.duration_ms, 1 if action.error else 0,
        ))
        action_id = cursor.lastrowid
        conn.commit()
    if action.has_file_content and action.file_content and action.file_path:
        background_tasks.add_task(
            _process_file_capture, action_id, action.file_path,
            action.file_content, action.session_id or "")
    # Track sequence
    _track_sequence(action.session_id, action.tool_name)
    return {"status": "logged", "action_id": action_id}


@router.post("/log/batch")
async def log_batch(batch: BatchActionLog, background_tasks: BackgroundTasks):
    """Batch log. Drop-in for Memory /api/observer/log/batch."""
    db = get_db()
    ids = []
    captures = []
    with db.get_connection() as conn:
        for a in batch.actions:
            cursor = conn.execute("""
                INSERT INTO observer_actions
                (timestamp, session_id, sequence_num, tool_name, server_name,
                 category, arguments_json, result_summary, has_file_content,
                 file_path, duration_ms, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id
            """, (
                a.timestamp or datetime.now(timezone.utc).isoformat(),
                a.session_id, a.sequence_num, a.tool_name, a.server_name,
                a.category, json.dumps(a.arguments)[:10000], a.result_summary,
                1 if a.has_file_content else 0, a.file_path,
                a.duration_ms, 1 if a.error else 0,
            ))
            aid = cursor.lastrowid
            ids.append(aid)
            if a.has_file_content and a.file_content and a.file_path:
                captures.append((aid, a.file_path, a.file_content, a.session_id or ""))
        conn.commit()
    for aid, fp, c, sid in captures:
        background_tasks.add_task(_process_file_capture, aid, fp, c, sid)
    return {"status": "logged", "count": len(ids), "file_captures": len(captures)}


@router.post("/exchange")
async def log_exchange(exchange: ExchangeLog):
    """Log a full conversation exchange."""
    db = get_db()
    with db.get_connection() as conn:
        conn.execute("""
            INSERT INTO observer_exchanges
            (session_id, prompt_preview, response_preview, model, tool_call_count, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (exchange.session_id, exchange.prompt[:500], exchange.response[:500],
              exchange.model, len(exchange.tool_calls),
              exchange.timestamp or datetime.now(timezone.utc).isoformat()))
        conn.commit()
    return {"status": "logged"}


@router.post("/webhook")
async def universal_webhook(payload: WebhookPayload, background_tasks: BackgroundTasks):
    """Universal intake — auto-classifies and routes everything."""
    content = payload.content
    file_path = payload.file_path or ""

    # Classify
    ct = payload.content_type
    if not ct:
        if file_path:
            ct = _classify_file(file_path, content)
        elif _looks_like_transcript(content):
            ct = "transcript"
        else:
            ct = "document"

    result = {"source": payload.source, "content_type": ct,
              "file_path": file_path, "char_count": len(content), "actions": []}

    if ct == "code":
        lang = _detect_language(file_path, content)
        if lang == "python" and SCAN_ENABLED:
            background_tasks.add_task(_scan_and_index, content, file_path, lang, payload.session_id or "")
            result["actions"].append("code_scan_queued")
        background_tasks.add_task(_index_document, content, file_path, "code", payload.session_id or "")
        result["actions"].append("document_index_queued")

    elif ct == "transcript":
        background_tasks.add_task(_ingest_transcript, content, payload.session_id or "", payload.source, payload.metadata)
        result["actions"].append("conversation_ingest_queued")

    elif ct in ("config", "infrastructure"):
        background_tasks.add_task(_index_document, content, file_path, ct, payload.session_id or "")
        result["actions"].append("document_index_queued")
        if payload.update_kb:
            facts = _extract_facts(content, file_path)
            if facts:
                stored = _store_facts(facts, file_path)
                result["facts_extracted"] = len(facts)
                result["facts_stored"] = stored
                result["actions"].append("facts_extracted")

    elif ct == "kb_change":
        background_tasks.add_task(_index_document, content, file_path, "kb_doc", payload.session_id or "")
        background_tasks.add_task(_reindex_kb_file, content, file_path)
        result["actions"].append("kb_reindex_queued")

    else:
        background_tasks.add_task(_index_document, content, file_path, ct, payload.session_id or "")
        result["actions"].append("document_index_queued")
        if payload.scan_code and _has_code_blocks(content):
            result["actions"].append("code_blocks_scan_queued")

    # Store file capture
    if file_path and content:
        db = get_db()
        with db.get_connection() as conn:
            conn.execute("""
                INSERT INTO observer_file_captures
                (file_path, content, language, content_type, char_count)
                VALUES (?, ?, ?, ?, ?)
            """, (file_path, content[:50000], _detect_language(file_path, content), ct, len(content)))
            conn.commit()

    # Meta event
    try:
        meta = get_meta_service()
        meta.write_meta("observer", f"webhook-{payload.source}", "intake", {
            "content_type": ct, "file_path": file_path,
            "char_count": len(content), "actions": result["actions"],
        }, written_by="observer_webhook")
    except Exception:
        pass

    return result


async def _scan_and_index(content, file_path, language, session_id):
    try:
        from services.scanner import get_scanner_service
        scanner = get_scanner_service()
        scanner.scan_source(content, language=language, filepath=file_path)
    except Exception as e:
        logger.warning(f"Scan failed: {e}")
    await _index_document(content, file_path, "code", session_id)


@router.get("/stats")
async def observer_stats():
    db = get_db()
    with db.get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM observer_actions")
        total = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM observer_actions WHERE error = 1")
        errors = c.fetchone()[0]
        c.execute("SELECT category, COUNT(*) FROM observer_actions GROUP BY category ORDER BY COUNT(*) DESC")
        by_cat = {r[0]: r[1] for r in c.fetchall()}
        c.execute("SELECT COUNT(*) FROM observer_file_captures")
        captures = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM observer_file_captures WHERE scanned = 1")
        scanned = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM observer_facts")
        facts = c.fetchone()[0]
        c.execute("SELECT fact_type, COUNT(*) FROM observer_facts GROUP BY fact_type ORDER BY COUNT(*) DESC")
        facts_by_type = {r[0]: r[1] for r in c.fetchall()}
        c.execute("SELECT COUNT(*) FROM observer_exchanges")
        exchanges = c.fetchone()[0]
        c.execute("SELECT tool_name, timestamp FROM observer_actions ORDER BY id DESC LIMIT 5")
        recent = [{"tool": r[0], "at": r[1]} for r in c.fetchall()]
    return {
        "actions": {"total": total, "errors": errors, "by_category": by_cat},
        "file_captures": {"total": captures, "scanned": scanned},
        "facts": {"total": facts, "by_type": facts_by_type},
        "exchanges": exchanges, "recent": recent,
    }


@router.get("/actions/recent")
async def recent_actions(limit: int = 20, category: Optional[str] = None):
    db = get_db()
    with db.get_connection() as conn:
        if category:
            cursor = conn.execute("""
                SELECT id, timestamp, session_id, tool_name, server_name, category, duration_ms, error
                FROM observer_actions WHERE category = ? ORDER BY id DESC LIMIT ?
            """, (category, min(limit, 100)))
        else:
            cursor = conn.execute("""
                SELECT id, timestamp, session_id, tool_name, server_name, category, duration_ms, error
                FROM observer_actions ORDER BY id DESC LIMIT ?
            """, (min(limit, 100),))
        actions = [{"id": r[0], "timestamp": r[1], "session_id": r[2], "tool_name": r[3],
                    "server_name": r[4], "category": r[5], "duration_ms": r[6], "error": bool(r[7])}
                   for r in cursor.fetchall()]
    return {"actions": actions, "count": len(actions)}


@router.get("/facts")
async def get_facts(fact_type: Optional[str] = None, limit: int = 50):
    db = get_db()
    with db.get_connection() as conn:
        if fact_type:
            cursor = conn.execute("""
                SELECT source_file, fact_type, fact_key, fact_value, confidence, created_at
                FROM observer_facts WHERE fact_type = ? ORDER BY created_at DESC LIMIT ?
            """, (fact_type, limit))
        else:
            cursor = conn.execute("""
                SELECT source_file, fact_type, fact_key, fact_value, confidence, created_at
                FROM observer_facts ORDER BY created_at DESC LIMIT ?
            """, (limit,))
        facts = [{"source_file": r[0], "fact_type": r[1], "fact_key": r[2],
                  "fact_value": r[3], "confidence": r[4], "created_at": r[5]}
                 for r in cursor.fetchall()]
    return {"facts": facts, "count": len(facts)}


# ---------------------------------------------------------------------------
# Phase 7E: Additional Query Endpoints
# ---------------------------------------------------------------------------

@router.get("/sequences")
async def get_sequences(min_count: int = 2, limit: int = 20):
    """Get recurring tool call sequences."""
    db = get_db()
    with db.get_connection() as conn:
        rows = conn.execute(
            """SELECT tool_sequence, sequence_hash, length, first_seen, last_seen, occurrence_count
               FROM observer_sequences
               WHERE occurrence_count >= ?
               ORDER BY occurrence_count DESC LIMIT ?""",
            (min_count, limit)
        ).fetchall()
        return {
            "count": len(rows),
            "sequences": [
                {
                    "tools": pg_sync.dejson(r[0]),
                    "hash": r[1], "length": r[2],
                    "first_seen": r[3], "last_seen": r[4],
                    "occurrences": r[5]
                } for r in rows
            ]
        }

@router.get("/tokens/{session_id}")
async def get_session_tokens(session_id: str):
    """Get token usage for a session."""
    db = get_db()
    with db.get_connection() as conn:
        rows = conn.execute(
            """SELECT exchange_num, tokens_in, tokens_out, tool_calls,
                      cumulative_in, cumulative_out, timestamp
               FROM observer_session_tokens WHERE session_id = ?
               ORDER BY exchange_num""",
            (session_id,)
        ).fetchall()
        if not rows:
            return {"session_id": session_id, "exchanges": 0, "total_tokens": 0}
        latest = rows[-1]
        return {
            "session_id": session_id,
            "exchanges": len(rows),
            "total_tokens": latest[4] + latest[5],
            "tokens_in": latest[4],
            "tokens_out": latest[5],
            "total_tool_calls": sum(r[3] for r in rows),
            "history": [
                {"exchange": r[0], "in": r[1], "out": r[2], "tools": r[3],
                 "cum_in": r[4], "cum_out": r[5], "ts": r[6]}
                for r in rows
            ]
        }

@router.post("/tokens")
async def record_tokens(session_id: str, exchange_num: int = 0,
                        tokens_in: int = 0, tokens_out: int = 0, tool_calls: int = 0):
    """Record token usage for a session exchange."""
    db = get_db()
    with db.get_connection() as conn:
        prev = conn.execute(
            "SELECT cumulative_in, cumulative_out FROM observer_session_tokens WHERE session_id = ? ORDER BY exchange_num DESC LIMIT 1",
            (session_id,)
        ).fetchone()
        cum_in = (prev[0] if prev else 0) + tokens_in
        cum_out = (prev[1] if prev else 0) + tokens_out
        conn.execute(
            "INSERT INTO observer_session_tokens (session_id, exchange_num, tokens_in, tokens_out, tool_calls, cumulative_in, cumulative_out, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
            (session_id, exchange_num, tokens_in, tokens_out, tool_calls, cum_in, cum_out,
             datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
    return {"status": "recorded", "cumulative_in": cum_in, "cumulative_out": cum_out}

@router.get("/exchanges/{session_id}")
async def get_session_exchanges(session_id: str, meaningful_only: bool = True):
    """Get all exchanges for a session."""
    db = get_db()
    with db.get_connection() as conn:
        where = "WHERE session_id = ?" + (" AND meaningful = 1" if meaningful_only else "")
        cursor = conn.execute(
            f"SELECT * FROM observer_exchanges {where} ORDER BY exchange_num",
            (session_id,)
        )
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description] if cursor.description and rows else []
        return {
            "session_id": session_id,
            "count": len(rows),
            "exchanges": [{cols[i]: r[i] for i in range(len(cols))} for r in rows] if cols else []
        }

@router.get("/captures/unscanned")
async def get_unscanned_captures(limit: int = 20):
    """Get file captures not yet scanned by Forge."""
    db = get_db()
    with db.get_connection() as conn:
        rows = conn.execute(
            "SELECT id, file_path, language, content_type, char_count, created_at FROM observer_file_captures WHERE scanned = 0 ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return {
            "count": len(rows),
            "captures": [{"id": r[0], "file_path": r[1], "language": r[2], "content_type": r[3], "chars": r[4], "captured_at": r[5]} for r in rows]
        }



# ---------------------------------------------------------------------------
# Snapshot Endpoints
# ---------------------------------------------------------------------------

@router.post("/snapshot/queue")
async def queue_snapshot_endpoint(component: str, reason: str = "manual"):
    """Queue a snapshot for a component."""
    try:
        from services.snapshots import queue_snapshot
        queued = queue_snapshot("components", component, reason)
        return {"status": "queued" if queued else "already_queued", "component": component}
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/snapshot/process")
async def process_snapshots_endpoint(limit: int = 5):
    """Process pending snapshots from the queue. Calls Haiku for each."""
    try:
        from services.snapshots import process_snapshot_queue
        result = await process_snapshot_queue(limit=limit)
        return result
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/snapshot/{component}")
async def get_snapshot_endpoint(component: str):
    """Get the most recent snapshot for a component."""
    try:
        from services.snapshots import get_snapshot
        snap = get_snapshot(component)
        if not snap:
            raise HTTPException(404, f"No snapshot found for: {component}")
        return snap
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/snapshots")
async def list_snapshots_endpoint():
    """List all components with snapshots."""
    from services.snapshots import list_snapshots
    return {"snapshots": list_snapshots()}
@router.post("/captures/{capture_id}/mark-scanned")
async def mark_capture_scanned(capture_id: int):
    """Mark a file capture as scanned."""
    db = get_db()
    with db.get_connection() as conn:
        conn.execute("UPDATE observer_file_captures SET scanned = 1 WHERE id = ?", (capture_id,))
        conn.commit()
    return {"status": "marked", "capture_id": capture_id}
