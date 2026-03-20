"""Pattern Event Handlers - pattern.detected subscriber

Fired by scanner when a NEW atom is created (action='created').
Subscribers:
- molecule_assembler  : find atoms with overlapping semantic tags -> propose molecule
- dictionary_updater  : add atom shorthand to compression dictionary
"""
import json
from services import pg_sync
import logging
import uuid
from typing import Any, Dict

log = logging.getLogger("helix.events.pattern")


async def handle_pattern_detected(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Handle pattern.detected event.

    Payload keys:
      atom_id       - new atom ID
      atom_name     - function name
      category      - semantic category (auth/api/data/util/middleware/...)
      structural_fp - structural fingerprint hash
      semantic_tags - list of semantic tags
      project       - source project
      filepath      - source filepath
    """
    atom_id       = payload.get("atom_id", "")
    atom_name     = payload.get("atom_name", "")
    category      = payload.get("category", "")
    semantic_tags = payload.get("semantic_tags", [])
    project       = payload.get("project", "")

    log.debug(f"pattern.detected: {atom_name} ({category}) -> {atom_id}")

    results = {}

    # ------------------------------------------------------------------ #
    # Molecule assembler - find co-occurring atoms with tag overlap
    # ------------------------------------------------------------------ #
    try:
        from services.database import get_db
        db = get_db()
        with db.get_connection() as conn:
            # Find recently active atoms (last 7 days) with semantic tag overlap
            recent_atoms = conn.execute(
                """SELECT id, name, meta FROM atoms
                   WHERE id != ? AND last_seen > datetime('now', '-7 days')
                   ORDER BY occurrence_count DESC LIMIT 30""",
                (atom_id,)
            ).fetchall()

            candidates = []
            for row_id, row_name, row_meta_str in recent_atoms:
                try:
                    row_meta = pg_sync.dejson(row_meta_str or "{}")
                    other_tags = row_meta.get("semantic", {}).get("tags", [])
                    overlap = set(semantic_tags) & set(other_tags)
                    if len(overlap) >= 2:
                        candidates.append({
                            "id": row_id,
                            "name": row_name,
                            "overlap_tags": sorted(overlap),
                            "overlap_count": len(overlap),
                        })
                except Exception:
                    pass

            # Sort by most overlap
            candidates.sort(key=lambda x: x["overlap_count"], reverse=True)

            if candidates:
                # Record molecule proposal in meta of new atom
                from services.meta import get_meta_service
                meta_svc = get_meta_service()
                meta_svc.write_meta("atoms", atom_id, "molecule_candidates", {
                    "candidates": candidates[:5],
                    "candidate_count": len(candidates),
                    "detected_at": "auto",
                }, written_by="pattern_events.molecule_assembler_v1")

                results["molecule_assembler"] = {
                    "status": "candidates_found",
                    "count": len(candidates),
                    "top": candidates[:3],
                }
                log.debug(f"Molecule candidates for {atom_name}: {len(candidates)} atoms")
            else:
                results["molecule_assembler"] = {"status": "no_candidates"}
    except Exception as e:
        log.debug(f"molecule_assembler failed (non-fatal): {e}")
        results["molecule_assembler"] = {"status": "error", "error": str(e)}

    # ------------------------------------------------------------------ #
    # Dictionary updater - add shorthand to compression dictionary
    # Only for categorised atoms with meaningful names (>=6 chars)
    # ------------------------------------------------------------------ #
    DICT_CATEGORIES = {"auth", "api", "data", "middleware", "util", "db", "cache",
                       "service", "router", "handler", "worker", "validator"}
    if category in DICT_CATEGORIES and atom_name and len(atom_name) >= 6:
        try:
            shorthand = f"{category}.{atom_name.lower()}"
            from services.database import get_db
            db = get_db()
            with db.get_connection() as conn:
                current = conn.execute(
                    "SELECT version, dictionary FROM dictionary_versions ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
                if current:
                    current_ver, current_dict_str = current
                    current_dict = pg_sync.dejson(current_dict_str or "{}")
                    if shorthand not in current_dict:
                        new_dict = {**current_dict, shorthand: atom_id}
                        # Parse version number from 'vN' format
                        try:
                            ver_num = int(current_ver.lstrip("v")) + 1
                        except ValueError:
                            ver_num = len(current_dict) + 1
                        new_ver = f"v{ver_num}"
                        delta = {shorthand: atom_id}
                        conn.execute(
                            """INSERT INTO dictionary_versions
                               (version, entries_count, dictionary, delta_from, delta)
                               VALUES (?, ?, ?, ?, ?)""",
                            (new_ver, len(new_dict), json.dumps(new_dict),
                             current_ver, json.dumps(delta))
                        )
                        conn.commit()
                        results["dictionary_updater"] = {
                            "status": "added",
                            "shorthand": shorthand,
                            "version": new_ver,
                            "total_entries": len(new_dict),
                        }
                        log.debug(f"Dictionary updated: {shorthand} -> {atom_id} ({new_ver})")
                    else:
                        results["dictionary_updater"] = {
                            "status": "exists",
                            "shorthand": shorthand,
                        }
        except Exception as e:
            log.debug(f"dictionary_updater failed (non-fatal): {e}")
            results["dictionary_updater"] = {"status": "error", "error": str(e)}
    else:
        results["dictionary_updater"] = {
            "status": "skipped",
            "reason": f"category={category} not in dict-eligible set or name too short",
        }

    return {
        "event": "pattern.detected",
        "atom_id": atom_id,
        "atom_name": atom_name,
        "results": results,
    }
