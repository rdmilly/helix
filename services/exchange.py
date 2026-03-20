"""Exchange Service — Per-Exchange Structured Observations

Every exchange between Claude and the user triggers a POST with:
- What changed (files, services, state deltas)
- Why (decisions, reasoning, constraints discovered)
- What was learned (failures, patterns, entities)
- Forward-looking (next steps, open questions, confidence)

The Mitochondria worker auto-routes exchange data into:
- structured_archive (decisions, failures, patterns)
- entities + relationships (knowledge graph)
- observer log (activity tracking)

Claude can skip with skip=True if the exchange is truly noise.
"""
import json
import logging
import re
import sqlite3
from services import pg_sync
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from services.database import get_db_path

log = logging.getLogger("helix.exchange")


def _conn():
    return pg_sync.sqlite_conn(str(get_db_path()), timeout=10)


def _now():
    return datetime.now(timezone.utc).isoformat()


def ensure_tables():
    """Schema already exists in PostgreSQL — no-op."""
    log.info("Exchange tables ready (PostgreSQL)")

def record_exchange(data: Dict[str, Any]) -> Dict[str, Any]:
    """Record a structured exchange observation.

    Returns the exchange ID and any intelligence extracted.
    """
    conn = _conn()
    try:
        exchange_id = uuid.uuid4().hex[:12]
        now = _now()
        skip = data.get("skip", False)

        # Always record the exchange (even skipped ones, for completeness)
        conn.execute("""
            INSERT INTO exchanges (
                id, session_id, exchange_num, timestamp,
                exchange_type, project, domain,
                files_changed, services_changed, state_before, state_after,
                decision, reason, rejected_alternatives, constraint_discovered,
                failure, pattern, entities_mentioned, relationships_found,
                next_step, open_questions, confidence,
                session_summary, session_goals, actions_taken,
                skip, tool_calls, tools_used, complexity,
                what_happened, notes
            ) VALUES (?,?,?,?, ?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?, ?,?,?,?, ?,?)
        """, (
            exchange_id,
            data.get("session_id", "unknown"),
            data.get("exchange_num", 0),
            now,
            data.get("exchange_type", "discuss"),
            data.get("project", ""),
            data.get("domain", ""),
            json.dumps(data.get("files_changed", [])),
            json.dumps(data.get("services_changed", [])),
            data.get("state_before", ""),
            data.get("state_after", ""),
            data.get("decision", ""),
            data.get("reason", ""),
            data.get("rejected_alternatives", ""),
            data.get("constraint_discovered", ""),
            data.get("failure", ""),
            data.get("pattern", ""),
            json.dumps(data.get("entities_mentioned", [])),
            json.dumps(data.get("relationships_found", [])),
            data.get("next_step", ""),
            json.dumps(data.get("open_questions", [])),
            data.get("confidence", 0.7),
            data.get("session_summary", ""),
            json.dumps(data.get("session_goals", [])),
            json.dumps(data.get("actions_taken", [])),
            1 if skip else 0,
            data.get("tool_calls", 0),
            json.dumps(data.get("tools_used", [])),
            data.get("complexity", "low"),
            data.get("what_happened", ""),
            data.get("notes", ""),
        ))


        conn.commit()

        # Now auto-route intelligence (only if not skipped)
        intelligence = {}
        if not skip:
            intelligence = _extract_intelligence(conn, data, exchange_id, now)
            conn.commit()

        # Execute requested actions
        actions_results = {}
        if not skip:
            actions_results = _execute_actions(conn, data, exchange_id, now)
            conn.commit()

        # Publish exchange.posted event (non-blocking, after commit)
        if not skip:
            try:
                from services.event_bus import publish
                publish("exchange.posted", {
                    "session_id": data.get("session_id", "unknown"),
                    "type": data.get("exchange_type", "discuss"),
                    "project": data.get("project", ""),
                    "content": data.get("what_happened", ""),
                    "exchange_id": exchange_id,
                    "decision": data.get("decision", ""),
                    "entities_mentioned": data.get("entities_mentioned", []),
                })
            except Exception as _be:
                log.debug(f"exchange.posted publish failed (non-fatal): {_be}")

        # Publish archive.recorded for each item added to structured_archive
        if not skip and intelligence:
            try:
                from services.event_bus import publish as _pub
                _archive_content_map = {
                    "decision_archived": ("decisions", data.get("decision", "")),
                    "failure_archived": ("failures", data.get("failure", "")),
                    "pattern_archived": ("patterns", data.get("pattern", "")),
                    "constraint_archived": ("patterns", data.get("constraint_discovered", "")),
                }
                for _key, (_coll, _content) in _archive_content_map.items():
                    _rec_id = intelligence.get(_key)
                    if _rec_id and _content:
                        _pub("archive.recorded", {
                            "record_id": _rec_id,
                            "collection": _coll,
                            "content": _content,
                            "session_id": data.get("session_id", "unknown"),
                            "exchange_id": exchange_id,
                        })
            except Exception as _ae:
                log.debug(f"archive.recorded publish failed (non-fatal): {_ae}")

        return {
            "status": "recorded",
            "id": exchange_id,
            "skip": skip,
            "intelligence": intelligence,
            "actions": actions_results,
        }
    except Exception as e:
        log.error(f"Exchange record failed: {e}")
        return {"status": "error", "error": str(e)}
    finally:
        conn.close()


def _extract_intelligence(
    conn: sqlite3.Connection,
    data: Dict[str, Any],
    exchange_id: str,
    timestamp: str,
) -> Dict[str, Any]:
    """Auto-route exchange data into archive, entities, relationships."""
    results = {}
    session_id = data.get("session_id", "unknown")

    # 1. Archive decisions
    decision = data.get("decision", "").strip()
    if decision:
        dec_id = uuid.uuid4().hex[:12]
        meta = {
            "exchange_id": exchange_id,
            "project": data.get("project", ""),
            "reason": data.get("reason", ""),
            "rejected": data.get("rejected_alternatives", ""),
            "confidence": data.get("confidence", 0.7),
        }
        conn.execute(
            "INSERT INTO structured_archive (id, collection, content, metadata_json, session_id, timestamp, created_at) VALUES (?,?,?,?,?,?,?)",
            (dec_id, "decisions", decision, json.dumps(meta), session_id, timestamp, timestamp)
        )
        results["decision_archived"] = dec_id

    # 2. Archive failures
    failure = data.get("failure", "").strip()
    if failure:
        fail_id = uuid.uuid4().hex[:12]
        meta = {
            "exchange_id": exchange_id,
            "project": data.get("project", ""),
            "constraint": data.get("constraint_discovered", ""),
        }
        conn.execute(
            "INSERT INTO structured_archive (id, collection, content, metadata_json, session_id, timestamp, created_at) VALUES (?,?,?,?,?,?,?)",
            (fail_id, "failures", failure, json.dumps(meta), session_id, timestamp, timestamp)
        )
        results["failure_archived"] = fail_id

    # 3. Archive patterns
    pattern = data.get("pattern", "").strip()
    if pattern:
        pat_id = uuid.uuid4().hex[:12]
        meta = {"exchange_id": exchange_id, "project": data.get("project", "")}
        conn.execute(
            "INSERT INTO structured_archive (id, collection, content, metadata_json, session_id, timestamp, created_at) VALUES (?,?,?,?,?,?,?)",
            (pat_id, "patterns", pattern, json.dumps(meta), session_id, timestamp, timestamp)
        )
        results["pattern_archived"] = pat_id

    # 4. Archive constraints as patterns too
    constraint = data.get("constraint_discovered", "").strip()
    if constraint:
        con_id = uuid.uuid4().hex[:12]
        meta = {"exchange_id": exchange_id, "project": data.get("project", ""), "source": "constraint"}
        conn.execute(
            "INSERT INTO structured_archive (id, collection, content, metadata_json, session_id, timestamp, created_at) VALUES (?,?,?,?,?,?,?)",
            (con_id, "patterns", f"CONSTRAINT: {constraint}", json.dumps(meta), session_id, timestamp, timestamp)
        )
        results["constraint_archived"] = con_id

    # 5. Upsert entities
    entities = data.get("entities_mentioned", [])
    entities_created = 0
    for ent in entities:
        if isinstance(ent, dict) and ent.get("name"):
            name = ent["name"]
            etype = ent.get("type", "unknown")
            desc = ent.get("description", "")
            existing = conn.execute("SELECT id FROM entities WHERE name = ?", (name,)).fetchone()
            if existing:
                conn.execute(
                    "UPDATE entities SET last_seen = ?, mention_count = COALESCE(mention_count,0)+1, entity_type = COALESCE(NULLIF(?,''), entity_type), description = COALESCE(NULLIF(?,''), description) WHERE name = ?",
                    (timestamp, etype, desc, name)
                )
            else:
                import hashlib
                eid = hashlib.sha256(f"{name}:{etype}".encode()).hexdigest()[:12]
                conn.execute(
                    "INSERT INTO entities (id, name, entity_type, description, attributes_json, first_seen, last_seen, meta, mention_count) VALUES (?,?,?,?,?,?,?,'{}',1)",
                    (eid, name, etype, desc, '{}', timestamp, timestamp)
                )
                entities_created += 1
    if entities:
        results["entities_processed"] = len(entities)
        results["entities_created"] = entities_created

    # 6. Create relationships
    relationships = data.get("relationships_found", [])
    rels_created = 0
    for rel in relationships:
        if isinstance(rel, dict) and rel.get("source") and rel.get("target"):
            conn.execute(
                "INSERT INTO kg_relationships (source_name, target_name, relation_type, description, session_id, created_at) VALUES (?,?,?,?,?,?)",
                (rel["source"], rel["target"], rel.get("type", "related_to"), rel.get("description", ""), session_id, timestamp)
            )
            rels_created += 1
    if relationships:
        results["relationships_created"] = rels_created

    return results



def _execute_actions(
    conn: sqlite3.Connection,
    data: Dict[str, Any],
    exchange_id: str,
    timestamp: str,
) -> Dict[str, Any]:
    """Execute any actions Claude requested during this exchange.

    Supported actions:
      - update_handoff: Write session summary to handoff doc
      - write_journal: Append to working journal
      - flag_alert: Record an alert/concern
      - archive_session: Snapshot session state into project_archive
    """
    results = {}
    actions = data.get("actions_taken", [])
    session_summary = data.get("session_summary", "")
    session_id = data.get("session_id", "unknown")
    project = data.get("project", "")

    for action in actions:
        if isinstance(action, str):
            action = {"type": action}
        if not isinstance(action, dict):
            continue

        atype = action.get("type", "")

        if atype == "update_handoff" and session_summary:
            # Write session summary to the exchanges table as a searchable record
            # The actual handoff file update happens via the summary field itself
            results["handoff_updated"] = True

        elif atype == "write_journal":
            entry = action.get("entry", session_summary or data.get("what_happened", ""))
            if entry:
                jid = uuid.uuid4().hex[:12]
                conn.execute(
                    "INSERT INTO structured_archive (id, collection, content, metadata_json, session_id, timestamp, created_at) VALUES (?,?,?,?,?,?,?)",
                    (jid, "sessions", entry, json.dumps({"exchange_id": exchange_id, "project": project, "type": "journal"}), session_id, timestamp, timestamp)
                )
                results["journal_written"] = jid

        elif atype == "flag_alert":
            alert_msg = action.get("message", "")
            severity = action.get("severity", "medium")
            if alert_msg:
                aid = uuid.uuid4().hex[:12]
                conn.execute(
                    "INSERT INTO structured_archive (id, collection, content, metadata_json, session_id, timestamp, created_at) VALUES (?,?,?,?,?,?,?)",
                    (aid, "failures", f"ALERT [{severity}]: {alert_msg}",
                     json.dumps({"exchange_id": exchange_id, "severity": severity, "project": project}),
                     session_id, timestamp, timestamp)
                )
                results["alert_flagged"] = aid

        elif atype == "archive_session":
            if session_summary:
                sid = uuid.uuid4().hex[:12]
                conn.execute(
                    "INSERT INTO structured_archive (id, collection, content, metadata_json, session_id, timestamp, created_at) VALUES (?,?,?,?,?,?,?)",
                    (sid, "project_archive", session_summary,
                     json.dumps({"exchange_id": exchange_id, "project": project, "goals": data.get("session_goals", [])}),
                     session_id, timestamp, timestamp)
                )
                results["session_archived"] = sid

    return results

def search_exchanges(
    query: str = "",
    project: str = "",
    exchange_type: str = "",
    limit: int = 20,
) -> Dict[str, Any]:
    """Search past exchanges."""
    conn = _conn()
    try:
        if query:
            sql = """SELECT e.* FROM exchanges e
                     WHERE e.search_vector @@ plainto_tsquery('english', ?) AND e.skip = 0"""
            params = [query]
            if project:
                sql += " AND e.project = ?"
                params.append(project)
            sql += " ORDER BY e.timestamp DESC LIMIT ?"
            params.append(limit)
        else:
            sql = "SELECT * FROM exchanges WHERE skip = 0"
            params = []
            if project:
                sql += " AND project = ?"
                params.append(project)
            if exchange_type:
                sql += " AND exchange_type = ?"
                params.append(exchange_type)
            sql += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)

        rows = conn.execute(sql, params).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            for key in ("files_changed", "services_changed", "entities_mentioned",
                        "relationships_found", "open_questions", "tools_used"):
                if key in d and isinstance(d[key], str):
                    try:
                        d[key] = pg_sync.dejson(d[key])
                    except:
                        pass
            results.append(d)

        return {"query": query, "count": len(results), "results": results}
    finally:
        conn.close()


def get_exchange_stats() -> Dict[str, Any]:
    """Get exchange statistics."""
    conn = _conn()
    try:
        total = conn.execute("SELECT COUNT(*) FROM exchanges").fetchone()[0]
        stored = conn.execute("SELECT COUNT(*) FROM exchanges WHERE skip = 0").fetchone()[0]
        by_type = {}
        for r in conn.execute("SELECT exchange_type, COUNT(*) FROM exchanges WHERE skip=0 GROUP BY exchange_type ORDER BY COUNT(*) DESC"):
            by_type[r[0]] = r[1]
        by_project = {}
        for r in conn.execute("SELECT project, COUNT(*) FROM exchanges WHERE skip=0 AND project != '' GROUP BY project ORDER BY COUNT(*) DESC LIMIT 20"):
            by_project[r[0]] = r[1]
        decisions = conn.execute("SELECT COUNT(*) FROM exchanges WHERE skip=0 AND decision != ''").fetchone()[0]
        failures = conn.execute("SELECT COUNT(*) FROM exchanges WHERE skip=0 AND failure != ''").fetchone()[0]
        return {
            "total": total, "stored": stored, "skipped": total - stored,
            "by_type": by_type, "by_project": by_project,
            "with_decisions": decisions, "with_failures": failures,
        }
    finally:
        conn.close()
