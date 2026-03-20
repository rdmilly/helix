from services import pg_sync
"""Cockpit Service — Phase 6: Nervous System Dashboard

Aggregates system-wide metrics, DNA stats, pipeline health,
anomaly/nudge feeds, and activity timeline for the Cockpit UI.

Provides read-only views across all Helix subsystems:
- System overview with health checks
- DNA growth metrics (atoms, molecules, organisms)
- Pipeline stats (queue throughput, compression ratios)
- Anomaly and nudge feeds with filtering
- Activity timeline from meta_events
"""
import logging
import sqlite3
from typing import Dict, Any, Optional, List
from datetime import datetime

from services.database import get_db

logger = logging.getLogger(__name__)


class CockpitService:
    """Nervous System dashboard data aggregator"""

    def __init__(self, db=None):
        self.db = db or get_db()

    # ── System Overview ─────────────────────────────────────

    def get_overview(self) -> Dict[str, Any]:
        """Full system overview: table counts, versions, capacity"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            # Table counts
            tables = {}
            for table in [
                "atoms", "molecules", "organisms", "sessions",
                "queue", "anomalies", "nudges", "decisions",
                "conventions", "entities", "compression_log",
                "meta_events", "meta_namespaces", "type_registry",
                "dictionary_versions", "intake_hashes"
            ]:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    tables[table] = cursor.fetchone()[0]
                except Exception:
                    tables[table] = -1

            # Namespace versions
            cursor.execute("SELECT namespace, version, registered_by FROM meta_namespaces ORDER BY namespace")
            namespaces = [
                {"namespace": r[0], "version": r[1], "registered_by": r[2]}
                for r in cursor.fetchall()
            ]

            # Queue breakdown
            cursor.execute("""
                SELECT status, COUNT(*) FROM queue GROUP BY status
            """)
            queue_breakdown = {r[0]: r[1] for r in cursor.fetchall()}

            # Dictionary info
            cursor.execute("""
                SELECT version, COUNT(*) as entries
                FROM dictionary_versions
                GROUP BY version
                ORDER BY version DESC
                LIMIT 1
            """)
            dict_row = cursor.fetchone()
            dictionary = {
                "version": dict_row[0] if dict_row else "none",
                "entries": dict_row[1] if dict_row else 0,
            }

            # Recent activity (last 24h)
            cursor.execute("""
                SELECT COUNT(*) FROM meta_events
                WHERE timestamp > datetime('now', '-24 hours')
            """)
            events_24h = cursor.fetchone()[0]

            cursor.execute("""
                SELECT COUNT(*) FROM queue
                WHERE status = 'completed' AND completed_at > datetime('now', '-24 hours')
            """)
            processed_24h = cursor.fetchone()[0]

            cursor.execute("""
                SELECT COUNT(*) FROM sessions
                WHERE created_at > datetime('now', '-24 hours')
            """)
            sessions_24h = cursor.fetchone()[0]

        return {
            "system": {
                "service": "helix-cortex",
                "phase": "Phase 6: Nervous System",
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
            "dna": {
                "atoms": tables.get("atoms", 0),
                "molecules": tables.get("molecules", 0),
                "organisms": tables.get("organisms", 0),
                "total": tables.get("atoms", 0) + tables.get("molecules", 0) + tables.get("organisms", 0),
            },
            "intelligence": {
                "anomalies": tables.get("anomalies", 0),
                "nudges": tables.get("nudges", 0),
                "decisions": tables.get("decisions", 0),
                "conventions": tables.get("conventions", 0),
                "entities": tables.get("entities", 0),
            },
            "pipeline": {
                "queue": queue_breakdown,
                "sessions": tables.get("sessions", 0),
                "intake_hashes": tables.get("intake_hashes", 0),
                "compression_events": tables.get("compression_log", 0),
                "meta_events": tables.get("meta_events", 0),
            },
            "infrastructure": {
                "namespaces": namespaces,
                "type_registry": tables.get("type_registry", 0),
                "dictionary": dictionary,
            },
            "activity_24h": {
                "meta_events": events_24h,
                "items_processed": processed_24h,
                "sessions_started": sessions_24h,
            },
        }

    # ── Anomalies ───────────────────────────────────────────
    # Schema: id, type, description, evidence, severity, state, session_id, created_at, resolved_at, meta

    def get_anomalies(
        self,
        severity: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """List anomalies with optional filtering"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            where_clauses = []
            params = []

            if severity:
                where_clauses.append("severity = ?")
                params.append(severity)
            if state:
                where_clauses.append("state = ?")
                params.append(state)

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

            # Count total
            cursor.execute(f"SELECT COUNT(*) FROM anomalies {where_sql}", params)
            total = cursor.fetchone()[0]

            # Fetch page
            cursor.execute(f"""
                SELECT id, type, severity, state, description,
                       evidence, session_id, created_at, resolved_at, meta
                FROM anomalies
                {where_sql}
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
            """, params + [limit, offset])

            anomalies = []
            for r in cursor.fetchall():
                anomalies.append({
                    "id": r[0],
                    "type": r[1],
                    "severity": r[2],
                    "state": r[3],
                    "description": r[4],
                    "evidence": r[5],
                    "session_id": r[6],
                    "created_at": r[7],
                    "resolved_at": r[8],
                    "meta": r[9],
                })

            # Severity breakdown
            cursor.execute("""
                SELECT severity, COUNT(*) FROM anomalies GROUP BY severity
            """)
            by_severity = {r[0]: r[1] for r in cursor.fetchall()}

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "by_severity": by_severity,
            "anomalies": anomalies,
        }

    # ── Nudges ──────────────────────────────────────────────
    # Schema: id, description, category, priority, state, session_id, created_at, resolved_at, meta

    def get_nudges(
        self,
        state: Optional[str] = None,
        category: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """List nudges with optional filtering"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            where_clauses = []
            params = []

            if state:
                where_clauses.append("state = ?")
                params.append(state)
            if category:
                where_clauses.append("category = ?")
                params.append(category)

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

            cursor.execute(f"SELECT COUNT(*) FROM nudges {where_sql}", params)
            total = cursor.fetchone()[0]

            cursor.execute(f"""
                SELECT id, description, category, priority, state,
                       session_id, created_at, resolved_at, meta
                FROM nudges
                {where_sql}
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
            """, params + [limit, offset])

            nudges = []
            for r in cursor.fetchall():
                nudges.append({
                    "id": r[0],
                    "description": r[1],
                    "category": r[2],
                    "priority": r[3],
                    "state": r[4],
                    "session_id": r[5],
                    "created_at": r[6],
                    "resolved_at": r[7],
                    "meta": r[8],
                })

            cursor.execute("""
                SELECT state, COUNT(*) FROM nudges GROUP BY state
            """)
            by_state = {r[0]: r[1] for r in cursor.fetchall()}

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "by_state": by_state,
            "nudges": nudges,
        }

    # ── Activity Timeline ───────────────────────────────────
    # Schema: id, target_table, target_id, namespace, action, old_value, new_value, written_by, timestamp

    def get_timeline(
        self,
        action: Optional[str] = None,
        target_table: Optional[str] = None,
        hours: int = 24,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Recent meta_events activity stream"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            where_clauses = [f"timestamp > datetime('now', '-{int(hours)} hours')"]
            params = []

            if action:
                where_clauses.append("action = ?")
                params.append(action)
            if target_table:
                where_clauses.append("target_table = ?")
                params.append(target_table)

            where_sql = f"WHERE {' AND '.join(where_clauses)}"

            cursor.execute(f"SELECT COUNT(*) FROM meta_events {where_sql}", params)
            total = cursor.fetchone()[0]

            cursor.execute(f"""
                SELECT id, action, target_table, target_id,
                       namespace, old_value, new_value,
                       written_by, timestamp
                FROM meta_events
                {where_sql}
                ORDER BY timestamp DESC
                LIMIT ? OFFSET ?
            """, params + [limit, offset])

            events = []
            for r in cursor.fetchall():
                events.append({
                    "id": r[0],
                    "action": r[1],
                    "target_table": r[2],
                    "target_id": r[3],
                    "namespace": r[4],
                    "old_value": r[5],
                    "new_value": r[6],
                    "written_by": r[7],
                    "timestamp": r[8],
                })

            # Action type breakdown
            cursor.execute(f"""
                SELECT action, COUNT(*) FROM meta_events
                {where_sql}
                GROUP BY action
            """, params)
            by_action = {r[0]: r[1] for r in cursor.fetchall()}

        return {
            "hours": hours,
            "total": total,
            "limit": limit,
            "offset": offset,
            "by_action": by_action,
            "events": events,
        }

    # ── DNA Stats ───────────────────────────────────────────

    def get_dna_stats(self) -> Dict[str, Any]:
        """Detailed DNA library statistics"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            # Atom details
            cursor.execute("""
                SELECT id, name, fp_version,
                       LENGTH(code) as code_size,
                       LENGTH(template) as template_size,
                       parameters_json,
                       first_seen
                FROM atoms
                ORDER BY first_seen DESC
            """)
            atoms = []
            for r in cursor.fetchall():
                param_count = 0
                if r[5]:
                    try:
                        import json
                        params = pg_sync.dejson(r[5])
                        param_count = len(params) if isinstance(params, (dict, list)) else 0
                    except Exception:
                        pass
                atoms.append({
                    "id": r[0],
                    "name": r[1],
                    "fp_version": r[2],
                    "code_size": r[3] or 0,
                    "template_size": r[4] or 0,
                    "param_count": param_count,
                    "first_seen": r[6],
                })

            # Molecule details
            cursor.execute("""
                SELECT id, name, description, atom_ids_json,
                       LENGTH(template) as template_size,
                       first_seen
                FROM molecules
                ORDER BY first_seen DESC
            """)
            molecules = []
            for r in cursor.fetchall():
                atom_count = 0
                if r[3]:
                    try:
                        import json
                        ids = pg_sync.dejson(r[3])
                        atom_count = len(ids) if isinstance(ids, list) else 0
                    except Exception:
                        pass
                molecules.append({
                    "id": r[0],
                    "name": r[1],
                    "description": r[2],
                    "atom_count": atom_count,
                    "template_size": r[4] or 0,
                    "first_seen": r[5],
                })

            # Organism count
            cursor.execute("SELECT COUNT(*) FROM organisms")
            organism_count = cursor.fetchone()[0]

            # Content type distribution (atoms may not have content_type)
            try:
                cursor.execute("""
                    SELECT content_type, COUNT(*) FROM atoms GROUP BY content_type
                """)
                by_content_type = {r[0]: r[1] for r in cursor.fetchall()}
            except Exception:
                by_content_type = {}

            # Total code volume
            cursor.execute("SELECT COALESCE(SUM(LENGTH(code)), 0) FROM atoms")
            total_code_bytes = cursor.fetchone()[0]

        return {
            "summary": {
                "atoms": len(atoms),
                "molecules": len(molecules),
                "organisms": organism_count,
                "total_code_bytes": total_code_bytes,
                "by_content_type": by_content_type,
            },
            "atoms": atoms,
            "molecules": molecules,
        }

    # ── Pipeline Stats ──────────────────────────────────────

    def get_pipeline_stats(self, hours: int = 24) -> Dict[str, Any]:
        """Queue throughput, compression ratios, session activity"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            time_filter = f"datetime('now', '-{int(hours)} hours')"

            # Queue throughput
            cursor.execute(f"""
                SELECT status, COUNT(*) FROM queue
                WHERE created_at > {time_filter}
                GROUP BY status
            """)
            queue_throughput = {r[0]: r[1] for r in cursor.fetchall()}

            # Overall queue
            cursor.execute("SELECT status, COUNT(*) FROM queue GROUP BY status")
            queue_total = {r[0]: r[1] for r in cursor.fetchall()}

            # Compression stats
            cursor.execute(f"""
                SELECT
                    COUNT(*) as events,
                    COALESCE(SUM(tokens_original_in), 0) as total_original,
                    COALESCE(SUM(tokens_compressed_in), 0) as total_compressed,
                    COALESCE(AVG(compression_ratio_in), 0) as avg_ratio,
                    MIN(compression_ratio_in) as best_ratio,
                    MAX(compression_ratio_in) as worst_ratio
                FROM compression_log
                WHERE timestamp > {time_filter}
            """)
            cr = cursor.fetchone()
            compression = {
                "events": cr[0],
                "tokens_original": cr[1],
                "tokens_compressed": cr[2],
                "tokens_saved": cr[1] - cr[2],
                "avg_ratio": round(cr[3], 4),
                "best_ratio": round(cr[4], 4) if cr[4] else 0,
                "worst_ratio": round(cr[5], 4) if cr[5] else 0,
            }

            # Session activity
            cursor.execute(f"""
                SELECT COUNT(*) FROM sessions
                WHERE created_at > {time_filter}
            """)
            sessions_period = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM sessions")
            sessions_total = cursor.fetchone()[0]

            # Dictionary versions
            cursor.execute("""
                SELECT version, entries_count, created_at
                FROM dictionary_versions
                ORDER BY created_at DESC
            """)
            dict_versions = [
                {"version": r[0], "entries": r[1], "created_at": r[2]}
                for r in cursor.fetchall()
            ]

            # Intake type distribution
            cursor.execute(f"""
                SELECT content_type, COUNT(*) FROM queue
                WHERE created_at > {time_filter}
                GROUP BY content_type
            """)
            intake_types = {r[0]: r[1] for r in cursor.fetchall()}

        return {
            "hours": hours,
            "queue": {
                "period": queue_throughput,
                "all_time": queue_total,
            },
            "compression": compression,
            "sessions": {
                "period": sessions_period,
                "all_time": sessions_total,
            },
            "dictionary_versions": dict_versions,
            "intake_types": intake_types,
        }


# ── Singleton ───────────────────────────────────────────────

_cockpit_service: Optional[CockpitService] = None


def get_cockpit_service() -> CockpitService:
    global _cockpit_service
    if _cockpit_service is None:
        _cockpit_service = CockpitService()
    return _cockpit_service
