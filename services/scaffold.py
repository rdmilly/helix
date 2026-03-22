"""Scaffold Engine — Phase 1d

Produces a scaffold object from 3 intent signals:
  1. message tokens    (what is the user asking for?)
  2. context path      (what file/dir is active?)
  3. recency window    (what was recently touched?)

Pipeline:
  parse_intent() → atom_match() → cascade_deps() → assemble()

Outputs:
  {
    atoms_matched: int,
    confidence: float,
    atoms: [{fingerprint, code, section, language, occurrences, source}],
    imports: [str],           # deduplicated import lines ready to paste
    boilerplate: str,         # top candidate function/class skeleton
    related_files: [str],     # files that likely need updating
    intent_tokens: [str],
    context_path: str | None,
    project: str | None,
  }

Two atom sources (merged + deduped):
  Forge FTS  → import-level patterns (1,208 atoms)
  pgvector   → function/class semantic matches (484 embeddings)
"""
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

log = logging.getLogger("helix.scaffold")

FORGE_URL = os.environ.get("FORGE_URL", "http://the-forge:9095")
EMBEDDINGS_URL = os.environ.get("EMBEDDINGS_URL", "http://helix-embeddings:8000")
HTTP_TIMEOUT = 10.0


# ---------------------------------------------------------------------------
# Intent parser
# ---------------------------------------------------------------------------

def parse_intent(intent_tokens: List[str], context_path: Optional[str],
                 project: Optional[str], recent_types: Optional[List[str]]) -> Dict[str, Any]:
    """Expand raw signals into structured intent."""
    tokens = [t.lower().strip() for t in (intent_tokens or []) if t.strip()]

    # Infer language from context path
    language = None
    if context_path:
        ext = Path(context_path).suffix.lower()
        language = {
            ".py": "python", ".js": "javascript", ".ts": "typescript",
            ".go": "go", ".rs": "rust", ".yaml": "yaml", ".yml": "yaml",
        }.get(ext)

    # Infer project from context path if not provided
    if not project and context_path:
        parts = Path(context_path).parts
        if "/opt/projects/" in context_path:
            idx = list(parts).index("projects") if "projects" in parts else -1
            if idx >= 0 and idx + 1 < len(parts):
                project = parts[idx + 1]

    # Classify intent type from tokens
    intent_type = "general"
    action_words = {"router", "endpoint", "api", "route"}
    db_words = {"database", "db", "postgres", "sql", "query", "model", "schema"}
    auth_words = {"auth", "login", "jwt", "token", "middleware", "permission"}
    test_words = {"test", "spec", "fixture", "mock"}
    if any(t in action_words for t in tokens):
        intent_type = "router"
    elif any(t in db_words for t in tokens):
        intent_type = "database"
    elif any(t in auth_words for t in tokens):
        intent_type = "auth"
    elif any(t in test_words for t in tokens):
        intent_type = "test"

    return {
        "tokens": tokens,
        "language": language,
        "project": project,
        "intent_type": intent_type,
        "recent_types": recent_types or [],
    }


# ---------------------------------------------------------------------------
# Forge FTS atom match
# ---------------------------------------------------------------------------

async def _forge_atoms(tokens: List[str], language: Optional[str],
                       limit: int) -> List[Dict[str, Any]]:
    """Query Forge suggest for each token, merge results."""
    seen_fps: set = set()
    atoms: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        for token in tokens[:4]:  # cap at 4 queries
            try:
                params: Dict = {"q": token, "limit": limit}
                if language:
                    params["language"] = language
                r = await client.get(f"{FORGE_URL}/api/forge/suggest", params=params)
                r.raise_for_status()
                data = r.json()
                for atom in data.get("atoms", []):
                    fp = atom.get("fingerprint")
                    if fp and fp not in seen_fps:
                        seen_fps.add(fp)
                        atoms.append({**atom, "source": "forge", "match_token": token})
            except Exception as e:
                log.warning(f"Forge suggest failed for '{token}': {e}")

    # Sort by occurrences descending
    atoms.sort(key=lambda a: a.get("occurrences", 0), reverse=True)
    return atoms[:limit]


# ---------------------------------------------------------------------------
# pgvector semantic atom match
# ---------------------------------------------------------------------------

async def _embed_query(query: str) -> Optional[List[float]]:
    """Embed a query string via helix-embeddings sidecar."""
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r = await client.post(
                f"{EMBEDDINGS_URL}/embed",
                json={"texts": [query]}
            )
            r.raise_for_status()
            data = r.json()
            embeddings = data.get("embeddings") or data.get("data", [])
            if embeddings:
                return embeddings[0]
    except Exception as e:
        log.warning(f"Embedding failed: {e}")
    return None


async def _pgvector_atoms(query: str, language: Optional[str],
                          project: Optional[str], limit: int) -> List[Dict[str, Any]]:
    """Semantic search over helix postgres atoms via pgvector."""
    embedding = await _embed_query(query)
    if not embedding:
        return []

    try:
        from services.pg_sync import get_pg_conn
        vec_str = "[" + ",".join(str(x) for x in embedding) + "]"
        lang_filter = "AND a.meta->>'language' = %(language)s" if language else ""
        proj_filter = "AND a.meta->>'project' = %(project)s" if project else ""

        with get_pg_conn(admin=True) as conn:
            rows = conn.execute(
                """SELECT a.id, a.name, a.code, a.occurrence_count,
                           a.full_name,
                           1 - (e.embedding <=> %(vec)s::vector) AS similarity
                   FROM atoms a
                   JOIN embeddings e ON e.source_type = 'atoms' AND e.source_id = a.id
                   ORDER BY e.embedding <=> %(vec)s::vector
                   LIMIT %(limit)s""",
                {"vec": vec_str, "limit": limit}
            ).fetchall()

        atoms = []
        for row in rows:
            atoms.append({
                "id": row[0], "name": row[1], "code": row[2],
                "occurrences": row[3], "full_name": row[4],
                "similarity": round(float(row[5]), 3), "source": "pgvector",
            })
        return atoms
    except Exception as e:
        log.warning(f"pgvector atom search failed: {e}")
        return []


# ---------------------------------------------------------------------------
# Dependency cascader — KG traversal
# ---------------------------------------------------------------------------

def _kg_related_files(context_path: Optional[str], project: Optional[str],
                      limit: int = 5) -> List[str]:
    """Traverse KG to find files likely related to context_path or project."""
    related: List[str] = []
    try:
        from services.neo4j_store import get_neo4j_store
        neo4j = get_neo4j_store()
        if not neo4j or not neo4j.is_connected():
            return related

        if context_path:
            # Find entities that share relationships with this file
            file_name = Path(context_path).name
            result = neo4j.query(
                """MATCH (f:Entity {name: $name})-[r]-(related:Entity)
                   WHERE related.entity_type IN ['file', 'module', 'service']
                   RETURN DISTINCT related.name AS name, related.entity_type AS type
                   LIMIT $limit""",
                {"name": file_name, "limit": limit}
            )
            for record in (result or []):
                name = record.get("name", "")
                if name and name not in related:
                    related.append(name)

        if project and len(related) < limit:
            # Find other files in same project that co-changed with similar patterns
            result = neo4j.query(
                """MATCH (e:Entity)-[:BELONGS_TO]->(p:Entity {name: $project})
                   WHERE e.entity_type IN ['file', 'module']
                   RETURN DISTINCT e.name AS name
                   LIMIT $limit""",
                {"project": project, "limit": limit - len(related)}
            )
            for record in (result or []):
                name = record.get("name", "")
                if name and name not in related:
                    related.append(name)

    except Exception as e:
        log.debug(f"KG traversal skipped: {e}")

    return related


# ---------------------------------------------------------------------------
# Scaffold assembler
# ---------------------------------------------------------------------------

def _extract_imports(atoms: List[Dict[str, Any]]) -> List[str]:
    """Pull import lines from forge atoms sorted by occurrence count."""
    imports = []
    seen = set()
    for atom in atoms:
        if atom.get("source") == "forge" and atom.get("section") == "imports":
            code = (atom.get("code") or "").strip()
            if code and code not in seen and code.startswith(("import ", "from ")):
                seen.add(code)
                imports.append(code)
    return imports


def _best_boilerplate(atoms: List[Dict[str, Any]]) -> str:
    """Return the highest-similarity pgvector function atom as boilerplate."""
    candidates = [
        a for a in atoms
        if a.get("source") == "pgvector" and a.get("code")
        and len(a.get("code", "")) > 30  # skip trivial one-liners
    ]
    if not candidates:
        return ""
    best = max(candidates, key=lambda a: a.get("similarity", 0))
    return best.get("code", "")


def _confidence(forge_atoms: List, pgvector_atoms: List,
                intent: Dict, related: List) -> float:
    """Score confidence 0..1 based on signal quality."""
    score = 0.0
    if forge_atoms:
        score += min(0.35, 0.05 * len(forge_atoms))
    if pgvector_atoms:
        top_sim = max((a.get("similarity", 0) for a in pgvector_atoms), default=0)
        score += top_sim * 0.4
    if related:
        score += min(0.15, 0.05 * len(related))
    if intent["intent_type"] != "general":
        score += 0.1  # bonus for classified intent
    return round(min(1.0, score), 3)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def build_scaffold(
    intent_tokens: Optional[List[str]] = None,
    context_path: Optional[str] = None,
    project: Optional[str] = None,
    recent_types: Optional[List[str]] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    """Build a full scaffold object from intent signals."""
    limit = max(1, min(limit, 20))
    intent = parse_intent(intent_tokens or [], context_path, project, recent_types)
    tokens = intent["tokens"]
    language = intent["language"]
    proj = intent["project"]

    # Run both matchers
    forge_atoms = await _forge_atoms(tokens, language, limit) if tokens else []
    query = " ".join(tokens) or (context_path or "")
    pgvec_atoms = await _pgvector_atoms(query, language, proj, limit) if query else []

    # Cascade deps from KG
    related_files = _kg_related_files(context_path, proj, limit=5)

    # Assemble final atom list: forge first (import patterns), then pgvector (functions)
    all_atoms = forge_atoms + [
        a for a in pgvec_atoms
        if not any(f.get("code") == a.get("code") for f in forge_atoms)
    ]

    return {
        "atoms_matched": len(all_atoms),
        "confidence": _confidence(forge_atoms, pgvec_atoms, intent, related_files),
        "atoms": all_atoms[:limit],
        "imports": _extract_imports(forge_atoms),
        "boilerplate": _best_boilerplate(pgvec_atoms),
        "related_files": related_files,
        "intent_tokens": tokens,
        "intent_type": intent["intent_type"],
        "context_path": context_path,
        "project": proj,
        "language": language,
    }
