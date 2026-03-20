"""Shard Assembler — Context Sharding via Diff Chains

Assembles context shards using diff chains instead of full state.
Token cost scales with recency of change, not project size.

Modes:
  DELTA:    base_summary + diffs since last session (small, fast)
  SNAPSHOT: full current state (first access or too much changed)
"""
import json
from services import pg_sync
import logging
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime

from services.database import get_db
from services.meta import get_meta_service
from services.diff import get_diff_service

logger = logging.getLogger(__name__)

DEFAULT_TOKEN_BUDGET = 2000
DELTA_BUDGET_THRESHOLD = 0.8


class ShardAssembler:
    """Assembles context shards from diff chains."""

    def __init__(self):
        self.db = get_db()
        self.meta = get_meta_service()
        self.diff = get_diff_service()

    def assemble_shard(
        self,
        target_type: str,
        target_id: str,
        since_session_id: Optional[str] = None,
        since_timestamp: Optional[str] = None,
        token_budget: int = DEFAULT_TOKEN_BUDGET,
        context_type: str = "session_start",
    ) -> Dict[str, Any]:
        """Assemble a context shard for an object.

        Returns delta shard (what changed) or snapshot (full state).
        """
        if since_session_id and not since_timestamp:
            since_timestamp = self._get_session_end_time(since_session_id)

        table = self._type_to_table(target_type)
        diff_chain = self.diff.get_diff_chain(table, target_id, since_timestamp=since_timestamp)
        delta_tokens = sum(d.get("diff_tokens", 0) for d in diff_chain)

        if not diff_chain or delta_tokens > token_budget * DELTA_BUDGET_THRESHOLD:
            return self._snapshot_shard(target_type, target_id, token_budget, context_type)
        else:
            return self._delta_shard(target_type, target_id, diff_chain, delta_tokens,
                                     token_budget, since_timestamp)

    def assemble_project_shard(
        self,
        project_name: str,
        since_session_id: Optional[str] = None,
        token_budget: int = DEFAULT_TOKEN_BUDGET,
    ) -> Dict[str, Any]:
        """Assemble shard for an entire project."""
        since_timestamp = None
        if since_session_id:
            since_timestamp = self._get_session_end_time(since_session_id)

        project_atoms = self._get_project_atoms(project_name, since_timestamp)
        project_sessions = self._get_project_sessions(project_name, since_timestamp)

        if not project_atoms and not project_sessions:
            return {"mode": "empty", "project": project_name, "token_budget_used": 0,
                    "content": {"message": f"No changes found for project '{project_name}'"}}

        delta_items = []
        tokens_used = 0
        per_item_budget = token_budget // max(len(project_atoms) + len(project_sessions), 1)

        for session in project_sessions[:5]:
            if tokens_used >= token_budget:
                break
            shard = self.assemble_shard("session", session["id"],
                                        since_timestamp=since_timestamp,
                                        token_budget=per_item_budget)
            if shard.get("token_budget_used", 0) > 0:
                delta_items.append(shard)
                tokens_used += shard.get("token_budget_used", 0)

        for atom in project_atoms[:10]:
            if tokens_used >= token_budget:
                break
            shard = self.assemble_shard("atom", atom["id"],
                                        since_timestamp=since_timestamp,
                                        token_budget=per_item_budget)
            if shard.get("token_budget_used", 0) > 0:
                shard["maturity"] = self.diff.get_maturity_score(atom["id"])
                delta_items.append(shard)
                tokens_used += shard.get("token_budget_used", 0)

        return {
            "mode": "project_delta",
            "project": project_name,
            "since_timestamp": since_timestamp,
            "items_included": len(delta_items),
            "token_budget": token_budget,
            "token_budget_used": tokens_used,
            "content": {
                "sessions": [i for i in delta_items if i.get("target_type") == "session"],
                "atoms": [i for i in delta_items if i.get("target_type") == "atom"],
            }
        }

    # ---- internal assembly ----

    def _delta_shard(self, target_type, target_id, diff_chain, delta_tokens,
                     token_budget, since_timestamp) -> Dict[str, Any]:
        table = self._type_to_table(target_type)
        base_summary = self._get_base_summary(table, target_id)
        delta_content = []
        tokens_remaining = token_budget - len(base_summary) // 4

        for diff in diff_chain:
            dt = diff.get("diff_tokens", 0)
            if dt <= tokens_remaining:
                delta_content.append({
                    "timestamp": diff.get("timestamp"),
                    "type": diff.get("diff_type"),
                    "summary": diff.get("summary", ""),
                    "is_revert": diff.get("is_revert", False),
                    "maturity_delta": diff.get("maturity_delta", 0.0),
                    "diff": diff.get("diff_content") if dt < 200 else None,
                })
                tokens_remaining -= dt

        maturity = self.diff.get_maturity_score(target_id) if target_type == "atom" else None

        return {
            "mode": "delta",
            "target_type": target_type,
            "target_id": target_id,
            "since_timestamp": since_timestamp,
            "diff_count": len(diff_chain),
            "maturity": maturity,
            "token_budget": token_budget,
            "token_budget_used": token_budget - tokens_remaining,
            "content": {"base_summary": base_summary, "delta": delta_content},
        }

    def _snapshot_shard(self, target_type, target_id, token_budget, context_type) -> Dict[str, Any]:
        table = self._type_to_table(target_type)
        snapshot = self._get_latest_snapshot(table, target_id)

        if snapshot:
            content = snapshot["content"]
            return {
                "mode": "snapshot",
                "target_type": target_type,
                "target_id": target_id,
                "snapshot_id": snapshot["id"],
                "snapshot_age_hours": snapshot.get("age_hours", 0),
                "token_budget": token_budget,
                "token_budget_used": min(len(json.dumps(content)) // 4, token_budget),
                "content": content,
            }

        content = self._build_inline_snapshot(table, target_id, token_budget)
        return {
            "mode": "snapshot_inline",
            "target_type": target_type,
            "target_id": target_id,
            "snapshot_id": None,
            "token_budget": token_budget,
            "token_budget_used": min(len(json.dumps(content)) // 4, token_budget),
            "content": content,
        }

    # ---- data accessors ----

    def _get_base_summary(self, table: str, record_id: str) -> str:
        snapshot = self._get_latest_snapshot(table, record_id)
        if snapshot:
            return snapshot["content"].get("summary", "")
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            if table == "atoms":
                cursor.execute("SELECT name, meta FROM atoms WHERE id=?", (record_id,))
                row = cursor.fetchone()
                if row:
                    meta = pg_sync.dejson(row[1] or "{}")
                    category = meta.get("semantic", {}).get("category", "")
                    return f"{row[0]} ({category})"
            elif table == "sessions":
                cursor.execute("SELECT meta FROM sessions WHERE id=?", (record_id,))
                row = cursor.fetchone()
                if row:
                    meta = pg_sync.dejson(row[0] or "{}")
                    return meta.get("analysis", {}).get("summary", "")[:300]
        return ""

    def _get_latest_snapshot(self, table: str, record_id: str) -> Optional[Dict[str, Any]]:
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    SELECT id, content, created_at FROM snapshots
                    WHERE target_table=? AND target_id=?
                    ORDER BY created_at DESC LIMIT 1
                """, (table, record_id))
                row = cursor.fetchone()
                if row:
                    now = datetime.utcnow()
                    created = datetime.fromisoformat(row[2]) if row[2] else now
                    age_hours = (now - created).total_seconds() / 3600
                    return {"id": row[0], "content": pg_sync.dejson(row[1] or "{}"),
                            "age_hours": round(age_hours, 1)}
            except Exception:
                pass
        return None

    def _build_inline_snapshot(self, table: str, record_id: str, token_budget: int) -> Dict[str, Any]:
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            if table == "atoms":
                cursor.execute("SELECT name, full_name, code, template, meta FROM atoms WHERE id=?",
                               (record_id,))
                row = cursor.fetchone()
                if row:
                    meta = pg_sync.dejson(row[4] or "{}")
                    return {
                        "name": row[0], "full_name": row[1],
                        "has_template": bool(row[3] and row[3] != row[2]),
                        "category": meta.get("semantic", {}).get("category", ""),
                        "tags": meta.get("semantic", {}).get("tags", []),
                        "maturity": self.diff.get_maturity_score(record_id),
                        "structural": meta.get("structural", {}),
                    }
            elif table == "sessions":
                cursor.execute("SELECT provider, model, meta FROM sessions WHERE id=?", (record_id,))
                row = cursor.fetchone()
                if row:
                    meta = pg_sync.dejson(row[2] or "{}")
                    return {
                        "provider": row[0], "model": row[1],
                        "summary": meta.get("analysis", {}).get("summary", "")[:500],
                        "entities": meta.get("entities", {}),
                        "decisions": meta.get("decisions", {}).get("items", []),
                    }
        return {"error": "Object not found"}

    def _get_session_end_time(self, session_id: str) -> Optional[str]:
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT updated_at FROM sessions WHERE id=?", (session_id,))
            row = cursor.fetchone()
            return row[0] if row else None

    def _get_project_atoms(self, project_name: str, since_timestamp: Optional[str]) -> List[Dict]:
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            if since_timestamp:
                cursor.execute("""
                    SELECT DISTINCT a.id, a.name FROM atoms a
                    JOIN meta_events me ON me.target_table='atoms' AND me.target_id=a.id
                    WHERE me.namespace='diff' AND me.timestamp>?
                      AND json_extract(a.meta,'$.provenance.projects') LIKE ?
                    ORDER BY me.timestamp DESC LIMIT 10
                """, (since_timestamp, f'%{project_name}%'))
            else:
                cursor.execute("""
                    SELECT id, name FROM atoms
                    WHERE json_extract(meta,'$.provenance.projects') LIKE ?
                    ORDER BY last_seen DESC LIMIT 10
                """, (f'%{project_name}%',))
            return [{"id": row[0], "name": row[1]} for row in cursor.fetchall()]

    def _get_project_sessions(self, project_name: str, since_timestamp: Optional[str]) -> List[Dict]:
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            if since_timestamp:
                cursor.execute("SELECT id FROM sessions WHERE created_at>? ORDER BY created_at DESC LIMIT 5",
                               (since_timestamp,))
            else:
                cursor.execute("SELECT id FROM sessions ORDER BY created_at DESC LIMIT 5")
            return [{"id": row[0]} for row in cursor.fetchall()]

    def _type_to_table(self, target_type: str) -> str:
        return {"atom": "atoms", "session": "sessions", "project": "atoms"}.get(target_type, "atoms")


    def assemble_recent_shard(
        self,
        hours: int = 168,
        token_budget: int = 1500,
        include_decisions: bool = True,
        include_entities: bool = True,
        include_atoms: bool = True,
        include_summaries: bool = True,
    ):
        """Assemble a session-start shard from recent activity.

        No target_id required. Returns recent decisions, key entities,
        stable atoms, and session summaries as injection text.
        Used by MemBrain extension once per conversation start.
        """
        from datetime import timedelta
        since_ts = None
        if hours:
            cutoff = datetime.utcnow() - timedelta(hours=hours)
            since_ts = cutoff.isoformat()

        lines_out = []
        token_used = 0

        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            # --- Decisions ---
            if include_decisions:
                if since_ts:
                    cursor.execute(
                        "SELECT decision, rationale, project FROM decisions "
                        "WHERE created_at >= ? ORDER BY created_at DESC LIMIT 10",
                        (since_ts,)
                    )
                else:
                    cursor.execute(
                        "SELECT decision, rationale, project FROM decisions "
                        "ORDER BY created_at DESC LIMIT 10"
                    )
                rows = cursor.fetchall()
                if rows:
                    lines_out.append("## Recent Decisions")
                    for dec, rat, proj in rows:
                        prefix = f"[{proj}] " if proj else ""
                        entry = f"- {prefix}{dec}"
                        if rat:
                            entry += f" ({rat[:80]})"
                        lines_out.append(entry)
                        token_used += len(entry) // 4
                    lines_out.append("")

            # --- Entities ---
            if include_entities:
                if since_ts:
                    cursor.execute(
                        "SELECT name, entity_type, description FROM entities "
                        "WHERE last_seen >= ? ORDER BY mention_count DESC LIMIT 8",
                        (since_ts,)
                    )
                else:
                    cursor.execute(
                        "SELECT name, entity_type, description FROM entities "
                        "ORDER BY mention_count DESC LIMIT 8"
                    )
                rows = cursor.fetchall()
                if rows:
                    lines_out.append("## Key Entities")
                    for name, etype, desc in rows:
                        entry = f"- {name} ({etype})"
                        if desc:
                            entry += f": {desc[:100]}"
                        lines_out.append(entry)
                        token_used += len(entry) // 4
                    lines_out.append("")

            # --- High-maturity atoms ---
            if include_atoms:
                cursor.execute(
                    "SELECT id, name, full_name FROM atoms "
                    "ORDER BY last_seen DESC LIMIT 20"
                )
                rows = cursor.fetchall()
                scored = []
                for atom_id, name, full_name in rows:
                    score = self.diff.get_maturity_score(atom_id)
                    scored.append((score, name, full_name))
                scored.sort(reverse=True)
                stable = [r for r in scored if r[0] >= 0.5][:6]
                if stable:
                    lines_out.append("## Stable Code Patterns")
                    for score, name, full_name in stable:
                        label = "verified" if score >= 0.85 else ("stable" if score >= 0.7 else "maturing")
                        lines_out.append(f"- {full_name or name} [{label} {score:.2f}]")
                        token_used += 8
                    lines_out.append("")

            # --- Session summaries ---
            if include_summaries:
                if since_ts:
                    cursor.execute(
                        "SELECT content FROM structured_archive "
                        "WHERE created_at >= ? ORDER BY created_at DESC LIMIT 3",
                        (since_ts,)
                    )
                else:
                    cursor.execute(
                        "SELECT content FROM structured_archive "
                        "ORDER BY created_at DESC LIMIT 3"
                    )
                rows = cursor.fetchall()
                if rows:
                    lines_out.append("## Recent Session Context")
                    for (content,) in rows:
                        summary = (content or "")[:200].replace("\n", " ")
                        if summary:
                            lines_out.append(f"- {summary}")
                            token_used += len(summary) // 4
                    lines_out.append("")

        text = "\n".join(lines_out).strip()
        return {
            "mode": "recent",
            "hours": hours,
            "token_budget": token_budget,
            "tokens_used": token_used,
            "injection_text": text,
            "has_content": bool(text),
        }

class SnapshotManager:
    """Creates base snapshots for objects with long diff chains."""

    def __init__(self):
        self.db = get_db()
        self.meta = get_meta_service()
        self.diff = get_diff_service()

    def process_snapshot_queue(self, limit: int = 20) -> Dict[str, Any]:
        """Process queued snapshot requests."""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    SELECT id, target_table, target_id, reason FROM snapshot_queue
                    WHERE processed_at IS NULL ORDER BY queued_at ASC LIMIT ?
                """, (limit,))
                queued = cursor.fetchall()
            except Exception:
                return {"processed": 0, "note": "snapshot_queue not ready"}

        results = []
        for row in queued:
            sq_id, table, record_id, reason = row
            result = self.take_snapshot(table, record_id, reason=reason)
            results.append(result)
            with self.db.get_connection() as conn:
                conn.cursor().execute(
                    "UPDATE snapshot_queue SET processed_at=NOW() WHERE id=?", (sq_id,))
                conn.commit()

        return {"processed": len(results), "results": results}

    def take_snapshot(self, table: str, record_id: str,
                      summary: Optional[str] = None, reason: str = "manual") -> Dict[str, Any]:
        """Take a base snapshot of an object's current state."""
        assembler = ShardAssembler()
        content = assembler._build_inline_snapshot(table, record_id, token_budget=4000)
        if not summary:
            summary = self._generate_summary(table, record_id, content)
        content["summary"] = summary
        content["snapshot_reason"] = reason
        content["snapshotted_at"] = datetime.utcnow().isoformat()

        snapshot_id = f"snap_{uuid.uuid4().hex[:12]}"
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO snapshots (id, target_table, target_id, content, created_at)
                    VALUES (?, ?, ?, ?, NOW())
                """, (snapshot_id, table, record_id, json.dumps(content)))
                conn.commit()
                return {"snapshot_id": snapshot_id, "table": table, "record_id": record_id}
            except Exception as e:
                return {"error": str(e)}

    def _generate_summary(self, table: str, record_id: str, content: Dict) -> str:
        if table == "atoms":
            name = content.get("name", record_id)
            category = content.get("category", "")
            maturity = content.get("maturity", 0.6)
            tags = ", ".join(content.get("tags", [])[:3])
            return f"{name} ({category}) — maturity {maturity:.2f} — {tags}"
        elif table == "sessions":
            return content.get("summary", "")[:200]
        return f"{table}.{record_id} snapshot"

_shard_assembler: Optional[ShardAssembler] = None
_snapshot_manager: Optional[SnapshotManager] = None

def get_shard_assembler() -> ShardAssembler:
    global _shard_assembler
    if _shard_assembler is None:
        _shard_assembler = ShardAssembler()
    return _shard_assembler

def get_snapshot_manager() -> SnapshotManager:
    global _snapshot_manager
    if _snapshot_manager is None:
        _snapshot_manager = SnapshotManager()
    return _snapshot_manager


class ShardDiffer:
    """Computes and stores diffs for file writes.

    Called by file_events.py handle_file_written subscriber.
    Compares new content against the last captured version in observer_file_captures.
    Stores result in shard_diffs table for session-level change tracking.
    """

    def __init__(self):
        """No-op: tables exist in PostgreSQL (migration 001)."""
        pass
_shard_differ: Optional["ShardDiffer"] = None


def get_shard_service() -> ShardDiffer:
    """Returns singleton ShardDiffer. Called by file_events.py on every file.written."""
    global _shard_differ
    if _shard_differ is None:
        _shard_differ = ShardDiffer()
    return _shard_differ
