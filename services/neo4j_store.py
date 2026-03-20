"""Neo4j Knowledge Graph Store — Phase 5.

Manages entity nodes and relationship edges in Neo4j Community.
Wired into:
  - membrane.py   : entity upsert path (write)
  - synapse.py    : context assembly (read — temporal traversal)

Schema:
  (:Entity {id, name, entity_type, description, first_seen, last_seen, mention_count})
  -[:RELATES_TO {relation_type, description, session_id, created_at}]->

Circuit breaker: 3 failures -> 120s cooldown, all calls degrade gracefully.
"""
import logging
import os
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

NEO4J_URI  = os.getenv("NEO4J_URI",  "bolt://helix-neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASSWORD", "0613b6ff20972862e43798fbdced449e")


class CircuitBreaker:
    def __init__(self, threshold: int = 3, timeout: int = 120):
        self.threshold   = threshold
        self.timeout     = timeout
        self.failures    = 0
        self.last_fail   = 0.0
        self.open        = False

    def record_failure(self):
        self.failures += 1
        self.last_fail  = time.time()
        if self.failures >= self.threshold:
            self.open = True
            logger.warning("Neo4j circuit breaker OPEN")

    def record_success(self):
        self.failures = 0
        if self.open:
            self.open = False
            logger.info("Neo4j circuit breaker CLOSED")

    def can_execute(self) -> bool:
        if not self.open:
            return True
        if time.time() - self.last_fail > self.timeout:
            self.open = False  # half-open
            return True
        return False


class Neo4jStore:
    """
    Async-compatible Neo4j store using the official Python driver.

    All write methods are synchronous (driver uses blocking sessions).
    Wrap in asyncio.to_thread() if called from async context.
    """

    def __init__(self):
        self._driver      = None
        self._initialized = False
        self.cb           = CircuitBreaker()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def initialize(self) -> bool:
        if not self.cb.can_execute():
            return False
        try:
            from neo4j import GraphDatabase
            self._driver = GraphDatabase.driver(
                NEO4J_URI,
                auth=(NEO4J_USER, NEO4J_PASS),
                connection_timeout=5,
                max_connection_lifetime=300,
            )
            # Verify connectivity
            self._driver.verify_connectivity()
            # Create indexes
            with self._driver.session() as s:
                s.run("CREATE INDEX entity_name IF NOT EXISTS FOR (e:Entity) ON (e.name)")
                s.run("CREATE INDEX entity_id   IF NOT EXISTS FOR (e:Entity) ON (e.id)")
                s.run("CREATE INDEX entity_type IF NOT EXISTS FOR (e:Entity) ON (e.entity_type)")
            self._initialized = True
            self.cb.record_success()
            logger.info(f"Neo4j connected: {NEO4J_URI}")
            return True
        except Exception as e:
            logger.error(f"Neo4j init failed: {e}")
            self.cb.record_failure()
            return False

    def close(self):
        if self._driver:
            try:
                self._driver.close()
            except Exception:
                pass
            self._driver = None
            self._initialized = False

    def _session(self):
        if not self._initialized or not self._driver:
            raise RuntimeError("Neo4j not initialized")
        return self._driver.session()

    # ------------------------------------------------------------------
    # Entity CRUD
    # ------------------------------------------------------------------

    def upsert_entity(self, entity: Dict[str, Any]) -> bool:
        """
        Create or update an :Entity node.
        entity keys: id, name, entity_type, description,
                     first_seen, last_seen, mention_count
        """
        if not self._initialized or not self.cb.can_execute():
            return False
        try:
            with self._session() as s:
                s.run("""
                    MERGE (e:Entity {id: $id})
                    SET   e.name          = $name,
                          e.entity_type  = $entity_type,
                          e.description  = $description,
                          e.first_seen   = $first_seen,
                          e.last_seen    = $last_seen,
                          e.mention_count = $mention_count
                """, {
                    "id":            entity.get("id", ""),
                    "name":          entity.get("name", ""),
                    "entity_type":   entity.get("entity_type", "unknown"),
                    "description":   entity.get("description") or "",
                    "first_seen":    str(entity.get("first_seen") or ""),
                    "last_seen":     str(entity.get("last_seen") or ""),
                    "mention_count": int(entity.get("mention_count") or 0),
                })
            self.cb.record_success()
            return True
        except Exception as e:
            logger.error(f"Neo4j upsert_entity failed: {e}")
            self.cb.record_failure()
            return False

    def upsert_relationship(self, rel: Dict[str, Any]) -> bool:
        """
        Create or update a [:RELATES_TO] edge between two :Entity nodes.
        rel keys: source_name, target_name, relation_type,
                  description, session_id, created_at
        Nodes are created (stub) if they don't exist.
        """
        if not self._initialized or not self.cb.can_execute():
            return False
        try:
            with self._session() as s:
                s.run("""
                    MERGE (a:Entity {name: $source})
                    MERGE (b:Entity {name: $target})
                    MERGE (a)-[r:RELATES_TO {relation_type: $rel_type}]->(b)
                    SET   r.description = $description,
                          r.session_id  = $session_id,
                          r.created_at  = $created_at
                """, {
                    "source":      rel.get("source_name", ""),
                    "target":      rel.get("target_name", ""),
                    "rel_type":    rel.get("relation_type", "RELATED"),
                    "description": rel.get("description") or "",
                    "session_id":  rel.get("session_id") or "",
                    "created_at":  str(rel.get("created_at") or ""),
                })
            self.cb.record_success()
            return True
        except Exception as e:
            logger.error(f"Neo4j upsert_relationship failed: {e}")
            self.cb.record_failure()
            return False

    # ------------------------------------------------------------------
    # Queries (for synapse context assembly)
    # ------------------------------------------------------------------

    def get_entity_neighbors(
        self,
        name: str,
        depth: int = 2,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        Return entities connected to `name` within `depth` hops.
        Returns list of {name, entity_type, relation_type, distance}.
        """
        if not self._initialized or not self.cb.can_execute():
            return []
        try:
            with self._session() as s:
                result = s.run("""
                    MATCH path = (start:Entity {name: $name})
                                 -[:RELATES_TO*1.." + str(depth) + "]->(neighbor:Entity)
                    WITH neighbor,
                         length(path) AS dist,
                         [r IN relationships(path) | r.relation_type] AS rel_chain
                    RETURN neighbor.name        AS name,
                           neighbor.entity_type AS entity_type,
                           rel_chain[-1]        AS relation_type,
                           dist
                    ORDER BY dist, neighbor.mention_count DESC
                    LIMIT $limit
                """.replace('" + str(depth) + "', str(depth)),
                    {"name": name, "limit": limit}
                )
                rows = [{"name": r["name"], "entity_type": r["entity_type"],
                         "relation_type": r["relation_type"], "distance": r["dist"]}
                        for r in result]
            self.cb.record_success()
            return rows
        except Exception as e:
            logger.error(f"Neo4j get_entity_neighbors failed: {e}")
            self.cb.record_failure()
            return []

    def find_path(self, source: str, target: str, max_depth: int = 4) -> List[str]:
        """Return shortest relationship path between two entities as list of names."""
        if not self._initialized or not self.cb.can_execute():
            return []
        try:
            with self._session() as s:
                result = s.run("""
                    MATCH p = shortestPath(
                        (a:Entity {name: $source})-[:RELATES_TO*.." + str(max_depth) + "]->(b:Entity {name: $target})
                    )
                    RETURN [n IN nodes(p) | n.name] AS path
                    LIMIT 1
                """.replace('" + str(max_depth) + "', str(max_depth)),
                    {"source": source, "target": target}
                )
                row = result.single()
            self.cb.record_success()
            return row["path"] if row else []
        except Exception as e:
            logger.error(f"Neo4j find_path failed: {e}")
            self.cb.record_failure()
            return []

    def search_entities(
        self,
        query: str,
        entity_type: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Full-text search on entity names/descriptions."""
        if not self._initialized or not self.cb.can_execute():
            return []
        try:
            with self._session() as s:
                if entity_type:
                    result = s.run("""
                        MATCH (e:Entity)
                        WHERE toLower(e.name) CONTAINS toLower($q)
                           OR toLower(e.description) CONTAINS toLower($q)
                        AND e.entity_type = $etype
                        RETURN e.id AS id, e.name AS name,
                               e.entity_type AS entity_type,
                               e.description AS description,
                               e.mention_count AS mention_count
                        ORDER BY e.mention_count DESC
                        LIMIT $limit
                    """, {"q": query, "etype": entity_type, "limit": limit})
                else:
                    result = s.run("""
                        MATCH (e:Entity)
                        WHERE toLower(e.name) CONTAINS toLower($q)
                           OR toLower(e.description) CONTAINS toLower($q)
                        RETURN e.id AS id, e.name AS name,
                               e.entity_type AS entity_type,
                               e.description AS description,
                               e.mention_count AS mention_count
                        ORDER BY e.mention_count DESC
                        LIMIT $limit
                    """, {"q": query, "limit": limit})
                rows = [{"id": r["id"], "name": r["name"],
                         "entity_type": r["entity_type"],
                         "description": r["description"],
                         "mention_count": r["mention_count"]}
                        for r in result]
            self.cb.record_success()
            return rows
        except Exception as e:
            logger.error(f"Neo4j search_entities failed: {e}")
            self.cb.record_failure()
            return []

    def stats(self) -> Dict[str, Any]:
        """Return node/edge counts."""
        if not self._initialized or not self.cb.can_execute():
            return {"available": False}
        try:
            with self._session() as s:
                nodes = s.run("MATCH (e:Entity) RETURN count(e) AS n").single()["n"]
                rels  = s.run("MATCH ()-[r:RELATES_TO]->() RETURN count(r) AS n").single()["n"]
            self.cb.record_success()
            return {"available": True, "entities": nodes, "relationships": rels}
        except Exception as e:
            logger.error(f"Neo4j stats failed: {e}")
            self.cb.record_failure()
            return {"available": False, "error": str(e)}


# Global singleton
_neo4j_store: Optional[Neo4jStore] = None


def get_neo4j_store() -> Neo4jStore:
    global _neo4j_store
    if _neo4j_store is None:
        _neo4j_store = Neo4jStore()
    return _neo4j_store
