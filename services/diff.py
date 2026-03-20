"""Diff Service — Delta Capture for Atoms, Sessions, Files, Entities

Captures the delta between versions of any Helix object.
Diffs stored as meta_events namespace entries — full change history
reconstructable from the event log.

Diff namespace in meta_events:
  namespace = 'diff'
  new_value = {
    'diff_type': 'code' | 'template' | 'structured',
    'from_version': str,    # hash of previous state
    'to_version': str,      # hash of new state
    'diff_content': str,    # unified diff string
    'diff_tokens': int,     # estimated token cost
    'summary': str,         # one-line description
    'maturity_delta': float,
    'is_revert': bool,
  }
"""
import difflib
import hashlib
import json
from services import pg_sync
import logging
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime

from services.database import get_db
from services.meta import get_meta_service

logger = logging.getLogger(__name__)

SNAPSHOT_THRESHOLD = 20
MATURITY_STABLE_BONUS = 0.02
MATURITY_CHANGE_PENALTY = 0.05
MATURITY_REVERT_PENALTY = 0.10
MATURITY_CONFIRM_BONUS = 0.15
MATURITY_MAX = 1.0
MATURITY_MIN = 0.0


class DiffService:
    """Computes, stores, and retrieves diffs for Helix objects."""

    def __init__(self):
        self.db = get_db()
        self.meta = get_meta_service()

    def compute_and_store_atom_diff(
        self,
        atom_id: str,
        old_code: str,
        new_code: str,
        old_template: Optional[str] = None,
        new_template: Optional[str] = None,
        written_by: str = "diff_service",
    ) -> Optional[Dict[str, Any]]:
        """Compute and store diff when an atom's code changes."""
        code_diff = self._unified_diff(old_code, new_code, label="code")
        if not code_diff:
            return None

        from_hash = self._content_hash(old_code)
        to_hash = self._content_hash(new_code)
        is_revert = self._is_revert(atom_id, to_hash)

        template_diff = None
        if old_template and new_template and old_template != new_template:
            template_diff = self._unified_diff(old_template, new_template, label="template")

        combined_diff = code_diff
        if template_diff:
            combined_diff += "\n--- template ---\n" + template_diff

        diff_tokens = len(combined_diff) // 4
        maturity_delta = -MATURITY_REVERT_PENALTY if is_revert else -MATURITY_CHANGE_PENALTY
        summary = self._summarize_diff(code_diff, is_revert)

        diff_data = {
            "diff_type": "code",
            "from_version": from_hash,
            "to_version": to_hash,
            "diff_content": combined_diff,
            "diff_tokens": diff_tokens,
            "summary": summary,
            "maturity_delta": maturity_delta,
            "is_revert": is_revert,
            "recorded_at": datetime.utcnow().isoformat(),
        }

        self.meta.write_meta("atoms", atom_id, "diff", diff_data, written_by=written_by)
        self._update_maturity(atom_id, maturity_delta)

        diff_count = self._count_diffs("atoms", atom_id)
        if diff_count >= SNAPSHOT_THRESHOLD:
            self._flag_for_snapshot("atoms", atom_id)

        logger.info(f"Atom diff stored: {atom_id} ({diff_tokens} tokens, revert={is_revert})")
        return diff_data

    def compute_and_store_template_diff(
        self,
        atom_id: str,
        old_template: str,
        new_template: str,
        written_by: str = "editor_service",
    ) -> Optional[Dict[str, Any]]:
        """Compute and store diff when an atom template changes."""
        diff = self._unified_diff(old_template, new_template, label="template")
        if not diff:
            return None

        diff_data = {
            "diff_type": "template",
            "from_version": self._content_hash(old_template),
            "to_version": self._content_hash(new_template),
            "diff_content": diff,
            "diff_tokens": len(diff) // 4,
            "summary": f"Template updated ({diff.count(chr(10))} lines changed)",
            "maturity_delta": 0.0,
            "is_revert": False,
            "recorded_at": datetime.utcnow().isoformat(),
        }

        self.meta.write_meta("atoms", atom_id, "template_diff", diff_data, written_by=written_by)
        logger.info(f"Template diff stored: {atom_id}")
        return diff_data

    def compute_and_store_session_diff(
        self,
        session_id: str,
        prev_session_id: Optional[str],
        new_entities: Dict[str, Any],
        new_decisions: List[str],
        written_by: str = "mitochondria_v1",
    ) -> Dict[str, Any]:
        """Compute what's new in this session vs the previous."""
        prev_entities: Dict[str, Any] = {}
        prev_decisions: List[str] = []

        if prev_session_id:
            try:
                prev_meta = self.meta.read_meta("sessions", prev_session_id, "entities")
                prev_entities = prev_meta or {}
                prev_dec = self.meta.read_meta("sessions", prev_session_id, "decisions")
                prev_decisions = prev_dec.get("items", []) if prev_dec else []
            except Exception:
                pass

        new_entity_types: Dict[str, List[str]] = {}
        for etype, enames in new_entities.items():
            if isinstance(enames, list):
                prev_list = prev_entities.get(etype, [])
                new_names = [n for n in enames if n not in prev_list]
                if new_names:
                    new_entity_types[etype] = new_names

        new_decision_items = [d for d in new_decisions if d not in prev_decisions]

        diff_data = {
            "diff_type": "structured",
            "from_session": prev_session_id,
            "to_session": session_id,
            "new_entities": new_entity_types,
            "new_decisions": new_decision_items,
            "entity_delta_count": sum(len(v) for v in new_entity_types.values()),
            "decision_delta_count": len(new_decision_items),
            "diff_tokens": self._estimate_structured_tokens(new_entity_types, new_decision_items),
            "recorded_at": datetime.utcnow().isoformat(),
        }

        self.meta.write_meta("sessions", session_id, "diff", diff_data, written_by=written_by)
        return diff_data

    def get_diff_chain(
        self,
        table: str,
        record_id: str,
        since_timestamp: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Retrieve diff chain for an object in chronological order."""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            if since_timestamp:
                cursor.execute("""
                    SELECT id, namespace, new_value, timestamp
                    FROM meta_events
                    WHERE target_table=? AND target_id=?
                      AND namespace IN ('diff','template_diff')
                      AND timestamp > ?
                    ORDER BY timestamp ASC LIMIT ?
                """, (table, record_id, since_timestamp, limit))
            else:
                cursor.execute("""
                    SELECT id, namespace, new_value, timestamp
                    FROM meta_events
                    WHERE target_table=? AND target_id=?
                      AND namespace IN ('diff','template_diff')
                    ORDER BY timestamp ASC LIMIT ?
                """, (table, record_id, limit))
            chain = []
            for row in cursor.fetchall():
                diff_data = pg_sync.dejson(row[2]) if row[2] else {}
                chain.append({"event_id": row[0], "namespace": row[1], "timestamp": row[3], **diff_data})
            return chain

    def get_maturity_score(self, atom_id: str) -> float:
        """Get maturity score for an atom (0.0 unstable, 1.0 verified)."""
        try:
            m = self.meta.read_meta("atoms", atom_id, "maturity")
            return m.get("score", 0.6)
        except Exception:
            return 0.6

    def record_usage(self, atom_id: str) -> float:
        """Record atom used without modification — builds maturity."""
        return self._update_maturity(atom_id, MATURITY_STABLE_BONUS)

    def record_confirmation(self, atom_id: str) -> float:
        """Explicit user confirmation — atom jumps to high maturity."""
        return self._update_maturity(atom_id, MATURITY_CONFIRM_BONUS, force_min=0.85)

    # ---- internals ----

    def _unified_diff(self, old: str, new: str, label: str = "content", context_lines: int = 3) -> str:
        old_lines = old.splitlines(keepends=True)
        new_lines = new.splitlines(keepends=True)
        diff = list(difflib.unified_diff(old_lines, new_lines,
                                         fromfile=f"{label}_before", tofile=f"{label}_after",
                                         n=context_lines))
        return "".join(diff)

    def _content_hash(self, content: str) -> str:
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _is_revert(self, atom_id: str, to_hash: str) -> bool:
        try:
            chain = self.get_diff_chain("atoms", atom_id)
            seen = {d.get("from_version") for d in chain}
            return to_hash in seen
        except Exception:
            return False

    def _summarize_diff(self, unified_diff: str, is_revert: bool) -> str:
        if is_revert:
            return "Reverted to earlier version (previous approach abandoned)"
        lines = unified_diff.split("\n")
        added = sum(1 for l in lines if l.startswith("+") and not l.startswith("+++ "))
        removed = sum(1 for l in lines if l.startswith("-") and not l.startswith("--- "))
        if added == 0 and removed > 0:
            return f"Removed {removed} lines"
        if removed == 0 and added > 0:
            return f"Added {added} lines"
        return f"+{added} -{removed} lines changed"

    def _estimate_structured_tokens(self, entities: Dict, decisions: List) -> int:
        return len(json.dumps({"entities": entities, "decisions": decisions})) // 4

    def _update_maturity(self, atom_id: str, delta: float, force_min: Optional[float] = None) -> float:
        try:
            current = self.get_maturity_score(atom_id)
            new_score = current + delta
            if force_min is not None:
                new_score = max(new_score, force_min)
            new_score = max(MATURITY_MIN, min(MATURITY_MAX, new_score))
            self.meta.write_meta("atoms", atom_id, "maturity", {
                "score": new_score, "prev_score": current, "delta": delta,
                "updated_at": datetime.utcnow().isoformat(),
            }, written_by="diff_service")
            return new_score
        except Exception as e:
            logger.warning(f"Maturity update failed for {atom_id}: {e}")
            return 0.6

    def _count_diffs(self, table: str, record_id: str) -> int:
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(created_at) FROM snapshots WHERE target_table=? AND target_id=?",
                           (table, record_id))
            row = cursor.fetchone()
            last_snapshot = row[0] if row and row[0] else "1970-01-01"
            cursor.execute("""
                SELECT COUNT(*) FROM meta_events
                WHERE target_table=? AND target_id=? AND namespace='diff' AND timestamp>?
            """, (table, record_id, last_snapshot))
            return cursor.fetchone()[0]

    def _flag_for_snapshot(self, table: str, record_id: str):
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO snapshot_queue (id, target_table, target_id, reason, queued_at)
                VALUES (?, ?, ?, 'diff_threshold', NOW())
            """, (f"sq_{uuid.uuid4().hex[:12]}", table, record_id))
            conn.commit()


_diff_service: Optional[DiffService] = None

def get_diff_service() -> DiffService:
    global _diff_service
    if _diff_service is None:
        _diff_service = DiffService()
    return _diff_service
