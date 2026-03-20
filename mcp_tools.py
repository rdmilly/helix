"""Helix Cortex — MCP Tools (baked-in)

Exposes Helix intelligence data as MCP tools via FastMCP,
mounted directly into the FastAPI app on /mcp.

Tools call existing service functions and DB layer directly —
no HTTP hop, no separate container.

Tools:
  helix_search_conversations  — Hybrid search past conversation transcripts
  helix_search_decisions      — Search decision archive
  helix_search_failures       — Search failure/error archive
  helix_search_patterns       — Search recurring pattern archive
  helix_search_archive        — Search all archive collections at once
  helix_search_kb             — Unified KB search (infra + working docs)
  helix_query_entity          — Look up a knowledge graph entity
  helix_search_entities       — Search knowledge graph entities
  helix_get_diagnostics       — System health and data inventory
  helix_observer_recent       — Recent tool call activity
"""

import json
from services import pg_sync
import logging
import os
from datetime import datetime, timezone
from typing import Optional
from mcp.server.fastmcp import FastMCP

from config import DB_PATH, FTS_DB_PATH

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FastMCP instance — mounted at /mcp in main.py
# ---------------------------------------------------------------------------
mcp = FastMCP(
    "helix_cortex",
    instructions=(
        "Helix Cortex is the unified intelligence backend for Millyweb infrastructure. "
        "It stores conversation transcripts, decisions, failures, patterns, knowledge graph "
        "entities, and unified KB docs. Use these tools to search and retrieve context."
    ),
)

# ---------------------------------------------------------------------------
# DB helpers (same pattern as routers — direct sqlite3)
# ---------------------------------------------------------------------------

def _cortex_conn():
    """Get pg_sync connection (archive, knowledge graph, KB, observer)."""
    return pg_sync.sqlite_conn()


def _fts_conn():
    """Get SQLite connection for BM25 conversation FTS."""
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(str(FTS_DB_PATH))
    conn.row_factory = _sqlite3.Row
    return conn


def _rows_to_list(rows, max_content=500):
    """Convert sqlite3.Row objects to dicts, truncating long content."""
    results = []
    for r in rows:
        d = dict(r)
        for key in ("metadata", "properties"):
            if key in d and isinstance(d[key], str):
                try:
                    d[key] = json.loads(d[key])
                except (json.JSONDecodeError, TypeError):
                    pass
        if "content" in d and isinstance(d["content"], str) and len(d["content"]) > max_content:
            d["content_preview"] = d["content"][:max_content] + "..."
            d["content_length"] = len(d["content"])
            del d["content"]
        results.append(d)
    return results


# ===========================================================================
# CONVERSATION SEARCH
# ===========================================================================

@mcp.tool(
    name="helix_search_conversations",
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def helix_search_conversations(
    query: str,
    limit: int = 10,
    source: Optional[str] = None,
) -> str:
    """Search past conversation transcripts using hybrid FTS5 + vector search.

    Finds relevant chunks from 500+ conversation sessions stored in Helix.
    Results ranked by Reciprocal Rank Fusion (keyword + semantic match).

    Args:
        query: What to search for (e.g., "traefik routing configuration")
        limit: Max results to return (1-20, default 10)
        source: Optional source filter
    Returns:
        JSON with matching conversation chunks, scores, and metadata.
    """
    from services import conversation_store

    limit = max(1, min(limit, 20))
    try:
        result = await conversation_store.hybrid_search(
            query=query,
            limit=limit,
            source_filter=source,
        )
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        logger.error(f"Conversation search failed: {e}")
        return json.dumps({"error": str(e), "query": query})


# ===========================================================================
# ARCHIVE SEARCH (decisions, failures, patterns)
# ===========================================================================

def _search_archive_db(query, collection, limit):
    """Shared archive search logic using PG tsvector."""
    conn = _cortex_conn()
    try:
        if collection:
            rows = conn.execute(
                """SELECT sa.* FROM structured_archive sa
                   WHERE sa.search_vector @@ plainto_tsquery('english', ?)
                     AND sa.collection = ?
                   ORDER BY sa.timestamp DESC LIMIT ?""",
                (query, collection, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT sa.* FROM structured_archive sa
                   WHERE sa.search_vector @@ plainto_tsquery('english', ?)
                   ORDER BY sa.timestamp DESC LIMIT ?""",
                (query, limit),
            ).fetchall()
        return _rows_to_list(rows)
    finally:
        conn.close()


@mcp.tool(
    name="helix_search_decisions",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
)
async def helix_search_decisions(query: str, limit: int = 10) -> str:
    """Search past architectural and design decisions.

    Finds decisions from previous sessions — architecture choices,
    technology selections, design tradeoffs, naming conventions, etc.

    Args:
        query: What to search for (e.g., "provisioner architecture")
        limit: Max results (1-50, default 10)
    """
    limit = max(1, min(limit, 50))
    try:
        results = _search_archive_db(query, "decisions", limit)
        return json.dumps({"query": query, "collection": "decisions", "count": len(results), "results": results}, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "query": query})


@mcp.tool(
    name="helix_search_failures",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
)
async def helix_search_failures(query: str, limit: int = 10) -> str:
    """Search past failures, errors, and issues.

    Finds problems from previous sessions — deployment errors, bugs,
    config mistakes, corruption events. Useful for avoiding repeated
    mistakes and finding prior fixes.

    Args:
        query: What to search for (e.g., "chromadb corruption")
        limit: Max results (1-50, default 10)
    """
    limit = max(1, min(limit, 50))
    try:
        results = _search_archive_db(query, "failures", limit)
        return json.dumps({"query": query, "collection": "failures", "count": len(results), "results": results}, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "query": query})


@mcp.tool(
    name="helix_search_patterns",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
)
async def helix_search_patterns(query: str, limit: int = 10) -> str:
    """Search recurring patterns and best practices.

    Finds patterns detected across sessions — common workflows,
    repeated approaches, infrastructure conventions.

    Args:
        query: What to search for (e.g., "docker compose")
        limit: Max results (1-50, default 10)
    """
    limit = max(1, min(limit, 50))
    try:
        results = _search_archive_db(query, "patterns", limit)
        return json.dumps({"query": query, "collection": "patterns", "count": len(results), "results": results}, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "query": query})


@mcp.tool(
    name="helix_search_archive",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
)
async def helix_search_archive(
    query: str,
    collection: Optional[str] = None,
    limit: int = 20,
) -> str:
    """Search the full structured archive across all collections.

    Collections: decisions, failures, patterns, sessions, project_archive,
    snapshots, entities. Omit collection to search everything.

    Args:
        query: What to search for
        collection: Optional filter (decisions/failures/patterns/sessions/etc)
        limit: Max results (1-50, default 20)
    """
    limit = max(1, min(limit, 50))
    try:
        results = _search_archive_db(query, collection, limit)
        return json.dumps({"query": query, "collection": collection, "count": len(results), "results": results}, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "query": query})


# ===========================================================================
# KB UNIFIED SEARCH
# ===========================================================================

@mcp.tool(
    name="helix_search_kb",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
)
async def helix_search_kb(
    query: str,
    source: Optional[str] = None,
    limit: int = 10,
) -> str:
    """Search all knowledge base documents (infrastructure + working docs).

    Searches 158+ indexed KB documents using FTS5. Covers server configs,
    MCP architecture, networking, projects, journals, changelogs, specs.

    Args:
        query: What to search for (e.g., "traefik routes")
        source: Optional filter — "infra-kb" or "working-kb"
        limit: Max results (1-50, default 10)
    """
    limit = max(1, min(limit, 50))
    conn = _cortex_conn()
    try:
        if source:
            rows = conn.execute(
                """SELECT kb.* FROM kb_documents kb
                   WHERE kb.search_vector @@ plainto_tsquery('english', ?)
                     AND kb.source = ?
                   ORDER BY kb.indexed_at DESC LIMIT ?""",
                (query, source, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT kb.* FROM kb_documents kb
                   WHERE kb.search_vector @@ plainto_tsquery('english', ?)
                   ORDER BY kb.indexed_at DESC LIMIT ?""",
                (query, limit),
            ).fetchall()
        results = _rows_to_list(rows)
        return json.dumps({"query": query, "source": source, "count": len(results), "results": results}, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "query": query})
    finally:
        conn.close()


# ===========================================================================
# KNOWLEDGE GRAPH
# ===========================================================================

@mcp.tool(
    name="helix_query_entity",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
)
async def helix_query_entity(name: str) -> str:
    """Look up a knowledge graph entity by exact name.

    Returns entity details, relationships, and mention count.
    Entities include containers, services, ports, domains, projects, tools, people.

    Args:
        name: Entity name (e.g., "helix-cortex", "mcp-provisioner", "VPS2")
    """
    conn = _cortex_conn()
    try:
        row = conn.execute(
            "SELECT * FROM entities WHERE name = ?", (name,)
        ).fetchone()
        if not row:
            return json.dumps({"error": f"Entity '{name}' not found", "suggestion": "Try helix_search_entities to find similar entities"})

        entity = dict(row)
        if isinstance(entity.get("properties"), str):
            try:
                entity["properties"] = json.loads(entity["properties"])
            except (json.JSONDecodeError, TypeError):
                pass

        rels = conn.execute(
            """SELECT * FROM kg_relationships
               WHERE source_entity = ? OR target_entity = ?
               ORDER BY created_at DESC LIMIT 20""",
            (name, name),
        ).fetchall()
        entity["relationships"] = [dict(r) for r in rels]

        mention_count = conn.execute(
            "SELECT COUNT(*) FROM kg_mentions WHERE entity_name = ?", (name,)
        ).fetchone()[0]
        entity["mention_count"] = mention_count

        return json.dumps(entity, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "name": name})
    finally:
        conn.close()


@mcp.tool(
    name="helix_search_entities",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
)
async def helix_search_entities(
    query: str,
    entity_type: Optional[str] = None,
    limit: int = 20,
) -> str:
    """Search knowledge graph entities by name or type.

    Types: container, service, port, domain, project, tool, person, database, network.

    Args:
        query: Search term (e.g., "mcp" or "postgres")
        entity_type: Optional type filter (e.g., "container", "service")
        limit: Max results (1-50, default 20)
    """
    limit = max(1, min(limit, 50))
    conn = _cortex_conn()
    try:
        if entity_type:
            rows = conn.execute(
                """SELECT * FROM entities
                   WHERE name LIKE ? AND entity_type = ?
                   ORDER BY mention_count DESC LIMIT ?""",
                (f"%{query}%", entity_type, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT * FROM entities
                   WHERE name LIKE ?
                   ORDER BY mention_count DESC LIMIT ?""",
                (f"%{query}%", limit),
            ).fetchall()
        results = _rows_to_list(rows, max_content=1000)
        return json.dumps({"query": query, "type": entity_type, "count": len(results), "results": results}, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "query": query})
    finally:
        conn.close()


# ===========================================================================
# DIAGNOSTICS & OBSERVER
# ===========================================================================

@mcp.tool(
    name="helix_get_diagnostics",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
)
async def helix_get_diagnostics() -> str:
    """Get full system health and data inventory.

    Returns table counts, database sizes, backup status, and overall health.
    Useful for understanding current state of Helix data stores.
    """
    # Use admin conn for diagnostics (bypasses RLS, gets real counts)
    conn = pg_sync.sqlite_conn(admin=True)
    try:
        tables = {}
        cursor = conn.execute(
            "SELECT table_name AS name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name"
        )
        for row in cursor:
            name = row["name"]
            try:
                count = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
                tables[name] = count
            except Exception:
                tables[name] = "error"

        db_sizes = {}
        try:
            sz = conn.execute("SELECT pg_database_size(current_database())").fetchone()[0]
            db_sizes["postgres/helix"] = f"{sz / (1024*1024):.2f} MB"
        except Exception:
            db_sizes["postgres/helix"] = "unknown"
        try:
            db_sizes["conversations_fts.db"] = f"{os.path.getsize(str(FTS_DB_PATH)) / (1024*1024):.2f} MB"
        except Exception:
            db_sizes["conversations_fts.db"] = "not found"

        backup_dir = DB_PATH.parent / "backups"
        backup_count = len(list(backup_dir.glob("*"))) if backup_dir.exists() else 0

        fts_conn = _fts_conn()
        try:
            chunk_count = fts_conn.execute("SELECT COUNT(*) FROM conversation_fts_content").fetchone()[0]
            session_count = fts_conn.execute("SELECT COUNT(DISTINCT c1) FROM conversation_fts_content WHERE c1 != ''").fetchone()[0]
        except Exception:
            chunk_count = "error"
            session_count = "error"
        finally:
            fts_conn.close()

        return json.dumps({
            "status": "healthy",
            "tables": tables,
            "table_count": len(tables),
            "db_sizes": db_sizes,
            "conversations": {"chunks": chunk_count, "sessions": session_count},
            "backups": backup_count,
        }, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        conn.close()


@mcp.tool(
    name="helix_observer_recent",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
)
async def helix_observer_recent(limit: int = 20) -> str:
    """Get recent tool call activity from the observer.

    Shows what tools were called, when, and by which session.
    Useful for understanding recent activity and debugging.

    Args:
        limit: Number of recent actions (1-100, default 20)
    """
    limit = max(1, min(limit, 100))
    conn = _cortex_conn()
    try:
        rows = conn.execute(
            """SELECT id, session_id, tool_name, server_name, timestamp,
                      duration_ms, error
               FROM observer_actions
               ORDER BY id DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        results = [{"id": r["id"], "session_id": r["session_id"],
                     "tool_name": r["tool_name"], "server_name": r["server_name"],
                     "timestamp": str(r["timestamp"]), "duration_ms": r["duration_ms"],
                     "error": bool(r["error"])} for r in rows]
        return json.dumps({"count": len(results), "actions": results}, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        conn.close()


# ============================================================
# WORKBENCH TOOLS (Editor Operations)
# ============================================================

@mcp.tool()
async def helix_file_write(
    path: str,
    content: str,
    context: str = "",
    scan: bool = None,
    index_kb: bool = None,
    extract_kg: bool = None,
    observe: bool = True,
    kb_source: str = None,
    kb_title: str = None,
) -> str:
    """Write a file and run the intelligence pipeline.

    The unified Editor operation. Writes the file, then automatically:
    - Scans code files for reusable patterns (atoms)
    - Indexes docs/configs into the knowledge base
    - Extracts knowledge graph entities (containers, ports, domains, projects)
    - Records the operation in the observer log

    Smart defaults: steps auto-detect based on file type.
    Override any step with explicit True/False.

    Args:
        path: Full VPS path (e.g., /opt/projects/helix/services/foo.py)
        content: File content to write
        context: What you're building/doing (helps KG extraction)
        scan: Scan for code patterns (auto: True for .py/.js/.ts)
        index_kb: Index into KB (auto: True for .md/.yml/.yaml/.json)
        extract_kg: Extract KG entities (auto: True always)
        observe: Record in observer log (default: True)
        kb_source: KB source tag (auto-detects from path)
        kb_title: Override KB document title
    Returns:
        JSON with results from each pipeline step.
    """
    import sys
    sys.path.insert(0, "/app")
    from services.workbench import pipeline
    try:
        result = await pipeline(
            path=path,
            content=content,
            context=context,
            do_write=True,
            do_scan=scan,
            do_kb_index=index_kb,
            do_kg_extract=extract_kg,
            do_observe=observe,
            kb_source=kb_source,
            kb_title=kb_title,
        )
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def helix_file_read(path: str, session_id: str = "workbench") -> str:
    """Read a file from the VPS.

    Returns file content with metadata (size, hash, type classification).
    Publishes a file.read event for write-on-touch epigenetic enrichment.

    Args:
        path: Full VPS path to read
        session_id: Session context for activity tracking (optional)
    Returns:
        JSON with content, size, hash, and file type.
    """
    import sys
    sys.path.insert(0, "/app")
    from services.workbench import read_file
    try:
        result = read_file(path, session_id=session_id)
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def helix_file_scan(content: str, path: str = "<scan>", language: str = None) -> str:
    """Scan content for reusable code patterns.

    Extracts atoms (function-level patterns) from source code.
    Currently supports Python. Does NOT write any files.

    Args:
        content: Source code to scan
        path: File path (for context/language detection)
        language: Override language detection (python, javascript, etc)
    Returns:
        JSON with atoms found (name, category, line count).
    """
    import sys
    sys.path.insert(0, "/app")
    from services.workbench import scan_code
    try:
        result = await scan_code(content, path, language)
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def helix_kb_index(
    content: str,
    path: str,
    source: str = "workbench",
    title: str = None,
) -> str:
    """Index a document into the unified knowledge base.

    Stores content in KB with FTS5 full-text search indexing.
    Does NOT write any files — only updates the KB database.
    Use this when you find a doc worth remembering.

    Args:
        content: Document content
        path: Logical path/identifier for the document
        source: Source tag (infra-kb, working-kb, workbench, etc)
        title: Document title (auto-extracted from first # heading if omitted)
    Returns:
        JSON with index status, document ID, title.
    """
    import sys
    sys.path.insert(0, "/app")
    from services.workbench import index_kb
    try:
        result = index_kb(content, path, source, title)
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def helix_file_list(path: str, pattern: str = "*", recursive: bool = False) -> str:
    """List files in a directory.

    Args:
        path: Directory path on VPS
        pattern: Glob pattern (default: * for all files)
        recursive: Search subdirectories too
    Returns:
        JSON with list of files and their metadata.
    """
    import sys
    sys.path.insert(0, "/app")
    import sys
    sys.path.insert(0, "/app")
    from pathlib import Path as P
    from services.workbench import classify_file
    try:
        d = P(path)
        if not d.exists():
            return json.dumps({"error": f"Directory not found: {path}"})
        if not d.is_dir():
            return json.dumps({"error": f"Not a directory: {path}"})
        method = d.rglob if recursive else d.glob
        files = []
        for f in sorted(method(pattern)):
            if f.is_file():
                try:
                    stat = f.stat()
                    files.append({
                        "path": str(f),
                        "name": f.name,
                        "size_bytes": stat.st_size,
                        "type": classify_file(str(f)),
                        "modified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                    })
                except Exception:
                    pass
            if len(files) >= 200:
                break
        return json.dumps({"directory": path, "count": len(files), "files": files}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ============================================================
# INTELLIGENCE WRITE TOOLS (Archive + Knowledge Graph)
# ============================================================

@mcp.tool()
async def helix_archive_record(
    collection: str,
    content: str,
    session_id: str = "claude",
    metadata: str = "{}",
) -> str:
    """Record an entry in the structured archive.

    Use this to permanently capture decisions, failures, patterns,
    and other significant events in the intelligence layer.

    Collections:
      - decisions: Architecture choices, tool selections, tradeoffs made
      - failures: Errors, bugs, things that went wrong and what was learned
      - patterns: Recurring approaches, solutions, workflows worth remembering
      - sessions: Session summaries
      - project_archive: Project milestones, completions, state snapshots
      - snapshots: Point-in-time system state captures

    Args:
        collection: Which collection to store in (decisions, failures, patterns, sessions, project_archive, snapshots)
        content: The content to archive — be descriptive
        session_id: Session identifier for grouping
        metadata: JSON string of extra metadata (optional)
    Returns:
        JSON with archive ID and status.
    """
    import uuid
    valid_collections = {"decisions", "failures", "patterns", "sessions", "project_archive", "snapshots"}
    if collection not in valid_collections:
        return json.dumps({"error": f"Invalid collection '{collection}'. Must be one of: {', '.join(sorted(valid_collections))}"})

    try:
        meta = json.loads(metadata) if metadata else {}
    except json.JSONDecodeError:
        meta = {"raw": metadata}

    conn = _cortex_conn()
    try:
        entry_id = uuid.uuid4().hex[:12]
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """INSERT INTO structured_archive (id, collection, content, metadata_json, session_id, timestamp, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (entry_id, collection, content, json.dumps(meta), session_id, now, now)
        )
        conn.commit()  # FTS updated by trigger
        # Publish archive.recorded event
        try:
            from services.event_bus import publish
            publish("archive.recorded", {
                "collection": collection, "content": content[:500],
                "entry_id": entry_id, "session_id": session_id,
            })
        except Exception:
            pass
        return json.dumps({"status": "recorded", "id": entry_id, "collection": collection, "size": len(content)})
    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        conn.close()


@mcp.tool()
async def helix_entity_upsert(
    name: str,
    entity_type: str,
    description: str = "",
    attributes: str = "{}",
) -> str:
    """Create or update an entity in the knowledge graph.

    Entities are the nodes of the intelligence layer — containers, services,
    projects, domains, people, tools, concepts. Use this to explicitly record
    what something is and why it matters.

    If the entity already exists (by name, case-insensitive), it updates
    the type, description, and attributes and bumps the mention count.

    Common entity types: container, service, project, domain, tool,
    mcp_server, person, concept, database, port

    Args:
        name: Entity name (e.g., "helix-cortex", "the-forge", "mcp.millyweb.com")
        entity_type: Type classification
        description: What this entity is / does
        attributes: JSON string of extra attributes (optional)
    Returns:
        JSON with upsert status.
    """
    import hashlib as _hashlib
    try:
        attrs = json.loads(attributes) if attributes else {}
    except json.JSONDecodeError:
        attrs = {"raw": attributes}

    conn = _cortex_conn()
    try:
        now = datetime.now(timezone.utc).isoformat()
        existing = conn.execute(
            "SELECT id, entity_type, description FROM entities WHERE name = ?", (name,)
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE entities SET
                   entity_type = COALESCE(?, entity_type),
                   description = COALESCE(NULLIF(?, ''), description),
                   attributes_json = ?,
                   last_seen = ?,
                   mention_count = COALESCE(mention_count, 0) + 1
                   WHERE name = ?""",
                (entity_type, description, json.dumps(attrs), now, name)
            )
            conn.commit()
            return json.dumps({"status": "updated", "name": name, "id": existing[0]})
        else:
            eid = _hashlib.sha256(f"{name}:{entity_type}".encode()).hexdigest()[:12]
            conn.execute(
                """INSERT INTO entities (id, name, entity_type, description, attributes_json, first_seen, last_seen, meta, mention_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 1)""",
                (eid, name, entity_type, description, json.dumps(attrs), now, now)
            )
            conn.commit()
            return json.dumps({"status": "created", "name": name, "id": eid, "type": entity_type})
    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        conn.close()


@mcp.tool()
async def helix_relationship_create(
    source: str,
    target: str,
    relation_type: str,
    description: str = "",
    session_id: str = "claude",
) -> str:
    """Create a relationship between two knowledge graph entities.

    Relationships connect entities with typed edges. Use this to record
    how things depend on, replace, contain, or relate to each other.

    Common relation types: depends_on, replaced_by, contains, part_of,
    connects_to, runs_on, manages, exposes, routes_to, serves

    Args:
        source: Source entity name (must exist or will be auto-created)
        target: Target entity name (must exist or will be auto-created)
        relation_type: Type of relationship (depends_on, replaced_by, etc.)
        description: Human description of why this relationship exists
        session_id: Session context
    Returns:
        JSON with relationship status.
    """
    conn = _cortex_conn()
    try:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """INSERT OR REPLACE INTO kg_relationships
               (source_name, target_name, relation_type, description, session_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (source, target, relation_type, description, session_id, now)
        )
        conn.commit()
        return json.dumps({
            "status": "created",
            "source": source,
            "target": target,
            "type": relation_type,
        })
    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        conn.close()


# ============================================================
# EXCHANGE TOOLS (Per-Exchange Observations)
# ============================================================

@mcp.tool()
async def helix_exchange_post(
    session_id: str = "claude",
    exchange_type: str = "discuss",
    project: str = "",
    domain: str = "",
    what_happened: str = "",
    files_changed: str = "[]",
    services_changed: str = "[]",
    state_before: str = "",
    state_after: str = "",
    decision: str = "",
    reason: str = "",
    rejected_alternatives: str = "",
    constraint_discovered: str = "",
    failure: str = "",
    pattern: str = "",
    entities_mentioned: str = "[]",
    relationships_found: str = "[]",
    next_step: str = "",
    open_questions: str = "[]",
    confidence: float = 0.7,
    complexity: str = "low",
    notes: str = "",
    tool_calls: int = 0,
    tools_used: str = "[]",
    session_summary: str = "",
    session_goals: str = "[]",
    actions_taken: str = "[]",
    skip: bool = False,
) -> str:
    """Post a structured exchange observation. Call after EVERY exchange.

    Auto-routes intelligence to the right places:
    - decision field → archived as a decision
    - failure field → archived as a failure
    - pattern/constraint fields → archived as patterns
    - entities_mentioned → upserted into knowledge graph
    - relationships_found → created in knowledge graph

    Fields:
      exchange_type: build, debug, plan, discuss, research, review, deploy
      domain: infra, code, business, content, personal
      what_happened: 1-2 sentence factual summary
      decision: What choice was made (empty if none)
      reason: Why this approach over alternatives
      failure: What broke and what was learned
      pattern: Recurring approach worth remembering
      constraint_discovered: New hard fact learned
      entities_mentioned: JSON array [{"name":"x","type":"y","description":"z"}]
      relationships_found: JSON array [{"source":"x","target":"y","type":"z"}]
      session_summary: Running narrative of what this session has accomplished so far
      session_goals: JSON array of session goals
      actions_taken: JSON array of actions to execute:
        - {"type": "update_handoff"} - mark handoff as updated
        - {"type": "write_journal", "entry": "..."} - write to session journal
        - {"type": "flag_alert", "message": "...", "severity": "high"} - flag an alert
        - {"type": "archive_session"} - snapshot session summary to project_archive
      skip: True to mark as noise (still recorded, not indexed)
    """
    import sys
    sys.path.insert(0, "/app")
    from services.exchange import record_exchange
    try:
        data = {
            "session_id": session_id,
            "exchange_type": exchange_type,
            "project": project,
            "domain": domain,
            "what_happened": what_happened,
            "files_changed": json.loads(files_changed) if files_changed else [],
            "services_changed": json.loads(services_changed) if services_changed else [],
            "state_before": state_before,
            "state_after": state_after,
            "decision": decision,
            "reason": reason,
            "rejected_alternatives": rejected_alternatives,
            "constraint_discovered": constraint_discovered,
            "failure": failure,
            "pattern": pattern,
            "entities_mentioned": json.loads(entities_mentioned) if entities_mentioned else [],
            "relationships_found": json.loads(relationships_found) if relationships_found else [],
            "next_step": next_step,
            "open_questions": json.loads(open_questions) if open_questions else [],
            "confidence": confidence,
            "complexity": complexity,
            "notes": notes,
            "session_summary": session_summary,
            "session_goals": json.loads(session_goals) if session_goals else [],
            "actions_taken": json.loads(actions_taken) if actions_taken else [],
            "tool_calls": tool_calls,
            "tools_used": json.loads(tools_used) if tools_used else [],
            "skip": skip,
        }
        result = record_exchange(data)
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def helix_exchange_search(
    query: str = "",
    project: str = "",
    exchange_type: str = "",
    limit: int = 20,
) -> str:
    """Search past exchange observations.

    FTS search across what_happened, decisions, reasons, failures,
    patterns, constraints, notes, and state changes.

    Args:
        query: FTS search query (empty returns recent)
        project: Filter by project name
        exchange_type: Filter by type (build, debug, plan, etc.)
        limit: Max results (default 20)
    """
    import sys
    sys.path.insert(0, "/app")
    from services.exchange import search_exchanges
    try:
        result = search_exchanges(query, project, exchange_type, limit)
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ============================================================
# LANGUAGE COMPRESSION TOOLS
# ============================================================

@mcp.tool()
async def helix_lang_compress(text: str, use_personal: bool = True) -> str:
    """Compress natural language text for token savings.

    Removes linguistic packaging (filler phrases, hedging, articles,
    ceremonial language) while preserving all semantic content.
    Deterministic — no LLM, no prediction, pure string transforms.

    Uses universal dictionary + personal frequency profile learned
    from conversation transcripts.

    Args:
        text: Text to compress
        use_personal: Include personal frequency patterns (default True)
    Returns:
        JSON with compressed text, token counts, savings.
    """
    from services.language_compression import get_language_compression
    try:
        svc = get_language_compression()
        result = svc.compress(text, use_personal=use_personal)
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def helix_lang_expand(text: str) -> str:
    """Expand compressed notation to natural readable English.

    Deterministic restoration — adds back articles, connectors,
    grammar that were removed during compression. No prediction.
    Used by Cortex as server-side fallback when browser extension
    is not available.

    Args:
        text: Compressed text to expand
    Returns:
        JSON with expanded text and token metrics.
    """
    from services.language_compression import get_language_compression
    try:
        svc = get_language_compression()
        result = svc.expand(text)
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def helix_lang_spec() -> str:
    """Get the compression injection spec for system prompts.

    Returns the ~138 token notation that teaches an LLM to output
    compressed. Inject into system prompt. The extension or Cortex
    handles expansion before the user sees output.

    Returns:
        JSON with spec text, token cost, version.
    """
    from services.language_compression import get_language_compression
    try:
        svc = get_language_compression()
        return json.dumps(svc.get_spec(), indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def helix_lang_test(text: str) -> str:
    """Test compress-expand roundtrip on sample text.

    Shows original, compressed, and expanded forms side by side
    with token counts. Use to verify expansion produces natural output.

    Args:
        text: Sample text to test
    Returns:
        JSON with original, compressed, expanded, and metrics.
    """
    from services.language_compression import get_language_compression
    try:
        svc = get_language_compression()
        result = svc.test_roundtrip(text)
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def helix_lang_analyze(min_frequency: int = 5) -> str:
    """Analyze conversation transcripts for compression opportunities.

    Runs frequency analysis across all stored conversation data.
    Returns personal compression profiles for both human and assistant,
    including top phrases, compressible patterns, and filler word counts.

    Args:
        min_frequency: Minimum occurrence count to include (default 5)
    Returns:
        JSON with human/assistant frequency profiles.
    """
    from services.language_compression import get_language_compression
    try:
        svc = get_language_compression()
        result = svc.analyze(min_frequency=min_frequency)
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def helix_compression_build(rebuild: bool = False) -> str:
    """Build or update personal compression profiles from transcript data.

    Analyzes conversation history to discover compressible patterns,
    track frequency across sessions, and promote patterns that cross
    thresholds. Profiles compound over time — each build discovers
    new patterns and reinforces existing ones.

    Lifecycle: candidate (5+) → active (20+, 3 sessions) → proven (50+, 10 sessions)

    Args:
        rebuild: Wipe and rebuild from scratch (default False = incremental)
    Returns:
        JSON with new patterns discovered, promotions, decay, and stats.
    """
    from services.compression_profiles import get_profile_service
    try:
        svc = get_profile_service()
        result = svc.build_profiles(rebuild=rebuild)
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def helix_compression_profiles() -> str:
    """Get summary of personal compression profiles.

    Shows pattern counts by stage, total tokens saved,
    and top 20 active patterns per role (human/assistant).
    Shows how compression has improved over time.

    Returns:
        JSON with profile summary and top patterns.
    """
    from services.compression_profiles import get_profile_service
    try:
        svc = get_profile_service()
        summary = svc.get_profile_summary()
        history = svc.get_compression_history()
        return json.dumps({"summary": summary, "history": history}, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def helix_scheduler_status() -> str:
    """Get status of Helix's built-in job scheduler.

    Shows all registered periodic jobs, their run schedule,
    last execution time, success/error counts.

    Jobs include: compression_profiles (daily), db_backup (6hr), pattern_decay (weekly)

    Returns:
        JSON with scheduler status and per-job details.
    """
    from services.scheduler import get_scheduler
    try:
        return json.dumps(get_scheduler().get_status(), indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def helix_scheduler_run(job_name: str) -> str:
    """Manually trigger a scheduled job immediately.

    Available jobs:
      compression_profiles - Rebuild personal compression profiles
      db_backup - Backup cortex.db
      pattern_decay - Check for stale patterns

    Args:
        job_name: Name of the job to run
    Returns:
        JSON with job result.
    """
    from services.scheduler import get_scheduler
    try:
        result = await get_scheduler().run_now(job_name)
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})
