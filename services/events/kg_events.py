from services import pg_sync
"""KG Event Handlers — entity.upserted subscriber

When an entity is upserted, subscribers can:
- Suggest relationships based on context
- Update cockpit display
- Trigger anomaly detection for unusual entity activity

Phase 1: cockpit notification + relationship suggestion stub.
"""
import logging
from typing import Any, Dict

log = logging.getLogger("helix.events.kg")


async def handle_entity_upserted(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Handle entity.upserted event.

    Published by routers/knowledge.py after entity create/update.
    Payload keys: name, entity_type, action (created/updated), description.
    """
    name = payload.get("name", "")
    entity_type = payload.get("entity_type", "")
    action = payload.get("action", "upserted")

    log.debug(f"entity.upserted: {name} ({entity_type}) — {action}")

    results = {}

    # --- Relationship suggester ---
    # Scan recent exchanges for entities co-mentioned with this one.
    # Auto-create kg_relationships if co-mention count >= 2.
    if name:
        try:
            import json as _json
            from services.database import get_db
            db = get_db()
            with db.get_connection() as conn:
                recent = conn.execute(
                    """SELECT id, entities_mentioned FROM exchanges
                       WHERE entities_mentioned LIKE ? ORDER BY created_at DESC LIMIT 30""",
                    (f'%{name}%',)
                ).fetchall()

                co_counts = {}
                for row_id, ent_json in recent:
                    try:
                        ents = pg_sync.dejson(ent_json or "[]")
                        for e in ents:
                            co_name = e.get("name", "") if isinstance(e, dict) else ""
                            if co_name and co_name != name:
                                co_counts[co_name] = co_counts.get(co_name, 0) + 1
                    except Exception:
                        pass

                created_rels = []
                for co_name, count in co_counts.items():
                    if count >= 2:
                        try:
                            conn.execute(
                                """INSERT INTO kg_relationships
                                   (source_name, target_name, relation_type,
                                    description, session_id, created_at)
                                   VALUES (?, ?, ?, ?, ?, NOW())""",
                                (name, co_name, "co_mentioned",
                                 f"Co-mentioned {count}x in recent exchanges",
                                 payload.get("session_id", "auto"))
                            )
                            created_rels.append({"target": co_name, "count": count})
                        except Exception:
                            pass

                if created_rels:
                    conn.commit()
                    results["relationship_suggester"] = {
                        "status": "ok",
                        "relationships_created": len(created_rels),
                        "detail": created_rels,
                    }
                    log.debug(f"KG: {len(created_rels)} relationships auto-created for {name}")
                else:
                    results["relationship_suggester"] = {
                        "status": "ok",
                        "no_strong_cooccurrences": True,
                    }
        except Exception as e:
            log.debug(f"relationship_suggester failed (non-fatal): {e}")
            results["relationship_suggester"] = {"status": "error", "error": str(e)}
    else:
        results["relationship_suggester"] = {"status": "skipped", "reason": "no entity name"}

    # --- Cockpit update ---
    # Cockpit reads entities directly from DB, so no push needed.
    # This is a hook for future websocket/SSE push.
    results["cockpit"] = {"status": "passive"}


    # --- Neo4j mirror ---
    try:
        from services.neo4j_store import get_neo4j_store
        neo4j = get_neo4j_store()
        if neo4j._initialized:
            neo4j.upsert_entity({
                "id":           payload.get("id", name),
                "name":         name,
                "entity_type":  entity_type,
                "description":  payload.get("description", ""),
                "first_seen":   payload.get("first_seen", ""),
                "last_seen":    payload.get("last_seen", ""),
                "mention_count": payload.get("mention_count", 0),
            })
            results["neo4j"] = {"status": "mirrored"}
        else:
            results["neo4j"] = {"status": "skipped", "reason": "not initialized"}
    except Exception as e:
        log.debug(f"Neo4j mirror failed (non-fatal): {e}")
        results["neo4j"] = {"status": "error", "error": str(e)}
    return {
        "event": "entity.upserted",
        "name": name,
        "entity_type": entity_type,
        "action": action,
        "results": results,
    }
