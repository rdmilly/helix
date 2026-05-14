import os
"""The Forge — Pattern Catalog Router.

Provides:
- forge_suggest: Semantic search over pattern catalog
- forge_compose: Assemble files from atoms/molecules
- Catalog browsing and statistics
"""

import json
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from services.database import get_db
from services.scanner import scan_file, extract_atoms

logger = logging.getLogger("forge.forge")
router = APIRouter(prefix="/api/forge", tags=["Forge"])

# -----------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------

class ScanRequest(BaseModel):
    file_path: str
    content: str
    language: Optional[str] = None
    project: Optional[str] = None
    source: str = "observer"  # observer, manual, workspace

class SuggestRequest(BaseModel):
    description: str
    language: Optional[str] = None
    limit: int = 10

# -----------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------

@router.post("/scan")
async def scan_content(req: ScanRequest):
    """Scan file content for atoms and catalog them.
    
    Called by:
    - Workspace (inline on every write)
    - Memory Observer (for captured file contents)
    - Manual trigger
    """
    language = req.language
    if not language:
        from routers.workspace import _detect_language
        language = _detect_language(req.file_path)
    if not language:
        return {"status": "skipped", "reason": "unknown language"}
    
    result = scan_file(req.file_path, req.content, language, req.project)
    return result


@router.get("/suggest")
async def suggest_patterns(
    q: str,
    language: Optional[str] = None,
    limit: int = 10
):
    """Search the pattern catalog for relevant atoms and molecules.
    
    Currently keyword-based. Will upgrade to semantic (ChromaDB) when
    the catalog is large enough to warrant embeddings.
    """
    db = get_db()
    try:
        # Search atoms by code content
        query = f"%{q.lower()}%"
        atoms_query = "SELECT * FROM atoms WHERE LOWER(code) LIKE ?"
        params = [query]
        if language:
            atoms_query += " AND language = ?"
            params.append(language)
        atoms_query += " ORDER BY occurrence_count DESC LIMIT ?"
        params.append(limit)
        
        atoms = db.execute(atoms_query, params).fetchall()
        
        # Search molecules by description or atom content
        mol_results = []
        molecules = db.execute(
            "SELECT * FROM molecules WHERE co_occurrence_count >= 2 ORDER BY co_occurrence_count DESC LIMIT 20"
        ).fetchall()
        
        for mol in molecules:
            # Check if any of the molecule's atoms match the query
            fps = json.loads(mol['atom_fingerprints_json'])
            matching_atoms = db.execute(
                f"SELECT code FROM atoms WHERE fingerprint IN ({','.join('?' * len(fps))}) AND LOWER(code) LIKE ?",
                (*fps, query)
            ).fetchall()
            if matching_atoms:
                mol_results.append({
                    "id": mol['id'],
                    "name": mol['name'],
                    "atom_count": mol['atom_count'],
                    "co_occurrences": mol['co_occurrence_count'],
                    "language": mol['language'],
                    "matching_atoms": [r['code'][:80] for r in matching_atoms]
                })
        
        return {
            "query": q,
            "atoms": [
                {
                    "fingerprint": a['fingerprint'],
                    "code": a['code'],
                    "section": a['section'],
                    "language": a['language'],
                    "occurrences": a['occurrence_count'],
                    "projects": json.loads(a['projects_json'])
                }
                for a in atoms
            ],
            "molecules": mol_results[:limit]
        }
    finally:
        db.close()


@router.get("/catalog/atoms")
async def list_atoms(
    language: Optional[str] = None,
    section: Optional[str] = None,
    min_count: int = 1,
    limit: int = 50,
    offset: int = 0
):
    """Browse the atom catalog."""
    db = get_db()
    try:
        query = "SELECT * FROM atoms WHERE occurrence_count >= ?"
        params = [min_count]
        if language:
            query += " AND language = ?"
            params.append(language)
        if section:
            query += " AND section = ?"
            params.append(section)
        query += " ORDER BY occurrence_count DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        rows = db.execute(query, params).fetchall()
        total = db.execute(
            "SELECT COUNT(*) as c FROM atoms WHERE occurrence_count >= ?", (min_count,)
        ).fetchone()['c']
        
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "atoms": [
                {
                    "fingerprint": r['fingerprint'],
                    "name": r['name'],
                    "code": r['code'],
                    "language": r['language'],
                    "section": r['section'],
                    "token_count": r['token_count'],
                    "occurrences": r['occurrence_count'],
                    "projects": json.loads(r['projects_json']),
                    "first_seen": r['first_seen'],
                    "last_seen": r['last_seen']
                }
                for r in rows
            ]
        }
    finally:
        db.close()


@router.get("/catalog/molecules")
async def list_molecules(
    min_count: int = 2,
    language: Optional[str] = None,
    limit: int = 20
):
    """Browse the molecule catalog."""
    db = get_db()
    try:
        query = "SELECT * FROM molecules WHERE co_occurrence_count >= ?"
        params = [min_count]
        if language:
            query += " AND language = ?"
            params.append(language)
        query += " ORDER BY co_occurrence_count DESC LIMIT ?"
        params.append(limit)
        
        rows = db.execute(query, params).fetchall()
        
        result = []
        for r in rows:
            fps = json.loads(r['atom_fingerprints_json'])
            # Fetch actual atom code for display
            atoms = db.execute(
                f"SELECT fingerprint, code, section FROM atoms WHERE fingerprint IN ({','.join('?' * len(fps))})",
                fps
            ).fetchall()
            result.append({
                "id": r['id'],
                "name": r['name'],
                "atom_count": r['atom_count'],
                "co_occurrences": r['co_occurrence_count'],
                "language": r['language'],
                "projects": json.loads(r['projects_json']),
                "atoms": [{"fingerprint": a['fingerprint'], "code": a['code'][:100], "section": a['section']} for a in atoms],
                "first_seen": r['first_seen'],
                "last_seen": r['last_seen']
            })
        
        return {"count": len(result), "molecules": result}
    finally:
        db.close()


@router.get("/catalog/stats")
async def catalog_stats():
    """Full catalog statistics."""
    db = get_db()
    try:
        atoms_total = db.execute("SELECT COUNT(*) as c FROM atoms").fetchone()['c']
        atoms_by_lang = db.execute(
            "SELECT language, COUNT(*) as c FROM atoms GROUP BY language ORDER BY c DESC"
        ).fetchall()
        atoms_by_section = db.execute(
            "SELECT section, COUNT(*) as c FROM atoms GROUP BY section ORDER BY c DESC"
        ).fetchall()
        mol_total = db.execute("SELECT COUNT(*) as c FROM molecules").fetchone()['c']
        mol_strong = db.execute("SELECT COUNT(*) as c FROM molecules WHERE co_occurrence_count >= 3").fetchone()['c']
        organisms_total = db.execute("SELECT COUNT(*) as c FROM organisms").fetchone()['c']
        scans = db.execute("SELECT COUNT(*) as c FROM scan_log").fetchone()['c']
        
        # Top atoms
        top_atoms = db.execute(
            "SELECT fingerprint, code, language, occurrence_count FROM atoms ORDER BY occurrence_count DESC LIMIT 10"
        ).fetchall()
        
        return {
            "atoms": {
                "total": atoms_total,
                "by_language": {r['language']: r['c'] for r in atoms_by_lang},
                "by_section": {r['section']: r['c'] for r in atoms_by_section},
                "top_10": [{"code": a['code'][:60], "lang": a['language'], "count": a['occurrence_count']} for a in top_atoms]
            },
            "molecules": {
                "total": mol_total,
                "strong (3+ co-occurrences)": mol_strong
            },
            "organisms": {
                "total": organisms_total
            },
            "total_scans": scans
        }
    finally:
        db.close()


@router.post("/ingest-existing")
async def ingest_existing_project(project_path: str):
    """Scan an existing project directory on the filesystem.
    
    Used to bootstrap the catalog from /opt/projects/* content.
    """
    from pathlib import Path as P
    from routers.workspace import _detect_language, _is_scannable
    
    base = P(project_path)
    if not base.exists():
        raise HTTPException(404, f"Path not found: {project_path}")
    
    project_name = base.name
    results = []
    scanned = 0
    skipped = 0
    
    for root, dirs, files in os.walk(base):
        # Skip common non-code dirs
        dirs[:] = [d for d in dirs if d not in {
            '__pycache__', 'node_modules', '.git', '.venv', 'venv',
            '.mypy_cache', '.pytest_cache', 'dist', 'build', 'assembled'
        }]
        
        for fname in files:
            fpath = P(root) / fname
            rel_path = str(fpath.relative_to(base))
            full_rel = f"{project_name}/{rel_path}"
            
            if not _is_scannable(str(fpath)):
                skipped += 1
                continue
            
            try:
                content = fpath.read_text(encoding='utf-8', errors='ignore')
                if len(content) < 10 or len(content) > 500_000:
                    skipped += 1
                    continue
                
                language = _detect_language(str(fpath))
                if not language:
                    skipped += 1
                    continue
                
                result = scan_file(full_rel, content, language, project_name)
                results.append(result)
                scanned += 1
            except Exception as e:
                logger.warning(f"Failed to scan {fpath}: {e}")
                skipped += 1
    
    return {
        "project": project_name,
        "scanned": scanned,
        "skipped": skipped,
        "atoms_found": sum(r.get('atoms_extracted', 0) for r in results if isinstance(r, dict)),
        "new_atoms": sum(r.get('new_atoms', 0) for r in results if isinstance(r, dict))
    }
