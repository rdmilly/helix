"""Session Buffer — non-blocking accumulation of authored session intelligence.

Claude authors intelligence DURING a session (decisions, failures, patterns,
entities, relationships, atoms, summary) instead of a model inferring it from a
transcript afterwards. Items land here immediately and cheaply; nothing is
routed downstream until the session is closed.

Design notes:
  * The buffer is EPHEMERAL working state, not durable intelligence, so it lives
    in its own SQLite file under the data dir (host bind mount) rather than in
    PostgreSQL. No migration, no risk to the intelligence layer.
  * Appends are NON-BLOCKING: validate shape, insert, return. No downstream
    routing, no LLM calls, no network.
  * Nothing is ever dropped silently (scar_helix_exchange_post_d07e). Every
    rejected item is returned with a reason and a truncated sample.
  * Every item carries provenance ('authored' by default). When haiku_reconciler
    is reconnected it can skip anything already authored instead of producing a
    second, inferred version of the same truth.
"""
import json
import logging
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger("helix.session_buffer")

ITEM_TYPES = {
    "decision", "failure", "pattern", "entity", "relationship",
    "atom", "summary", "action", "file",
}
PROVENANCE = {"authored", "inferred"}

# Routed through services.exchange.record_exchange at flush (inherits its gate).
_EXCHANGE_ROUTED = {"decision", "failure", "pattern", "entity", "relationship",
                    "summary", "action", "file"}

DATA_DIR = os.environ.get("HELIX_DATA_DIR", "/app/data")
BUFFER_DB = os.path.join(DATA_DIR, "session_buffer.db")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _conn():
    conn = sqlite3.connect(BUFFER_DB, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_tables() -> None:
    with _conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS session_buffer (
                id           TEXT PRIMARY KEY,
                session_id   TEXT NOT NULL,
                project      TEXT,
                item_type    TEXT NOT NULL,
                payload      TEXT NOT NULL,
                provenance   TEXT NOT NULL DEFAULT 'authored',
                status       TEXT NOT NULL DEFAULT 'buffered',
                detail       TEXT,
                created_at   TEXT NOT NULL,
                flushed_at   TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS ix_sb_session ON session_buffer(session_id, status)")
    log.info("session_buffer ready at %s", BUFFER_DB)


def _sample(value: Any, limit: int = 160) -> Any:
    if isinstance(value, (dict, list)):
        try:
            return json.loads(json.dumps(value)[:limit])
        except Exception:
            return str(value)[:limit]
    return str(value)[:limit]


def _validate(item: Any) -> Optional[str]:
    """Return a rejection reason, or None if the item is acceptable."""
    if not isinstance(item, dict):
        return "expected object with item_type/payload, got %s" % type(item).__name__
    itype = item.get("item_type")
    if itype not in ITEM_TYPES:
        return "unknown item_type %r (expected one of: %s)" % (
            itype, ", ".join(sorted(ITEM_TYPES)))
    payload = item.get("payload")
    if payload in (None, "", {}, []):
        return "empty payload for item_type %r" % itype
    if itype == "relationship":
        if not isinstance(payload, dict):
            return "relationship payload must be an object with source/target/relation_type"
        if not payload.get("source") or not payload.get("target"):
            return "relationship payload missing source and/or target"
    if itype == "entity":
        if not isinstance(payload, dict) or not payload.get("name"):
            return "entity payload must be an object with a name"
    if itype == "atom":
        if not isinstance(payload, dict) or not payload.get("name") or not payload.get("content"):
            return "atom payload must be an object with name and content"
    return None


def append(session_id: str, items: List[Any], project: str = "",
           provenance: str = "authored") -> Dict[str, Any]:
    """Validate and buffer items. Never routes downstream. Never raises on bad input."""
    if provenance not in PROVENANCE:
        provenance = "authored"
    if not isinstance(items, list):
        items = [items]

    accepted, rejected = [], []
    rows = []
    ts = _now()
    for idx, item in enumerate(items):
        reason = _validate(item)
        if reason:
            rejected.append({"index": idx, "reason": reason, "input": _sample(item)})
            continue
        item_id = uuid.uuid4().hex[:12]
        rows.append((item_id, session_id, project, item["item_type"],
                     json.dumps(item["payload"]), provenance, "buffered", None, ts, None))
        accepted.append({"id": item_id, "item_type": item["item_type"]})

    if rows:
        with _conn() as conn:
            conn.executemany(
                "INSERT INTO session_buffer (id, session_id, project, item_type, payload,"
                " provenance, status, detail, created_at, flushed_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)", rows)

    result = {
        "session_id": session_id,
        "submitted": len(items),
        "accepted": len(accepted),
        "items": accepted,
    }
    if rejected:
        result["rejected"] = rejected
    return result


def status(session_id: str) -> Dict[str, Any]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT item_type, status, COUNT(*) AS n FROM session_buffer"
            " WHERE session_id = ? GROUP BY item_type, status", (session_id,)).fetchall()
    by_type: Dict[str, Dict[str, int]] = {}
    total = 0
    for r in rows:
        by_type.setdefault(r["item_type"], {})[r["status"]] = r["n"]
        total += r["n"]
    return {"session_id": session_id, "total": total, "by_type": by_type}


def _buffered(session_id: str) -> List[sqlite3.Row]:
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM session_buffer WHERE session_id = ? AND status = 'buffered'"
            " ORDER BY created_at", (session_id,)).fetchall()


def _mark(ids: List[str], status_value: str, detail: str = "") -> None:
    if not ids:
        return
    with _conn() as conn:
        conn.executemany(
            "UPDATE session_buffer SET status = ?, detail = ?, flushed_at = ? WHERE id = ?",
            [(status_value, detail, _now(), i) for i in ids])


def flush(session_id: str, dry_run: bool = False) -> Dict[str, Any]:
    """Route buffered items into the durable intelligence layer.

    Exchange-routed types go through services.exchange.record_exchange, which
    already fans out to structured_archive + the knowledge graph and reports
    rejects. Atoms are held (status 'pending_catalog') until the authored-atom
    catalog path is wired — held and reported, never silently dropped.
    """
    rows = _buffered(session_id)
    if not rows:
        return {"session_id": session_id, "flushed": 0, "note": "nothing buffered"}

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    ids_by_type: Dict[str, List[str]] = {}
    project = ""
    for r in rows:
        grouped.setdefault(r["item_type"], []).append(json.loads(r["payload"]))
        ids_by_type.setdefault(r["item_type"], []).append(r["id"])
        project = project or (r["project"] or "")

    report: Dict[str, Any] = {
        "session_id": session_id,
        "buffered": len(rows),
        "by_type": {k: len(v) for k, v in grouped.items()},
        "dry_run": dry_run,
    }

    held = grouped.get("atom", [])
    if held:
        report["atoms_held"] = {
            "count": len(held),
            "reason": "authored-atom catalog path not wired yet; helix_scan requires Haiku."
                      " Held in buffer as pending_catalog — not dropped.",
        }

    if dry_run:
        report["would_route"] = sorted(t for t in grouped if t in _EXCHANGE_ROUTED)
        return report

    payload: Dict[str, Any] = {
        "session_id": session_id,
        "project": project,
        "exchange_type": "session_close",
        "provenance": "authored",
    }
    if grouped.get("summary"):
        first = grouped["summary"][0]
        payload["session_summary"] = first.get("text", first) if isinstance(first, dict) else first
    for itype, key in (("decision", "decision"), ("failure", "failure"), ("pattern", "pattern")):
        vals = grouped.get(itype) or []
        if vals:
            first = vals[0]
            payload[key] = first.get("text", first) if isinstance(first, dict) else first
    if grouped.get("entity"):
        payload["entities_mentioned"] = [
            e.get("name") if isinstance(e, dict) else e for e in grouped["entity"]]
    if grouped.get("relationship"):
        payload["relationships_found"] = grouped["relationship"]
    if grouped.get("action"):
        payload["actions_taken"] = grouped["action"]
    if grouped.get("file"):
        payload["files_changed"] = grouped["file"]

    routed_ids = [i for t, ids in ids_by_type.items() if t in _EXCHANGE_ROUTED for i in ids]
    try:
        from services import exchange as exchange_service
        result = exchange_service.record_exchange(payload)
        report["exchange"] = result
        _mark(routed_ids, "flushed", "routed via record_exchange")
        report["flushed"] = len(routed_ids)
    except Exception as exc:            # never lose the buffer on a routing failure
        log.exception("session_buffer flush failed for %s", session_id)
        _mark(routed_ids, "buffered", "flush failed: %s" % exc)
        report["flushed"] = 0
        report["error"] = str(exc)

    if held:
        _mark(ids_by_type.get("atom", []), "pending_catalog", "awaiting authored-atom path")
    return report
