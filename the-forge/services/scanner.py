"""The Forge — Atom scanner.

Decomposes file content into atoms (smallest reusable code fragments).
Fingerprints each atom and stores in the catalog.
Detects molecule bonds (atoms that co-occur across files).
"""

import hashlib
import json
import logging
import re
from pathlib import Path

from services.database import get_db

logger = logging.getLogger("forge.scanner")

# -----------------------------------------------------------------------
# Language-aware atom extraction
# -----------------------------------------------------------------------

# Import patterns
IMPORT_PATTERNS = {
    'python': [
        re.compile(r'^(from\s+\S+\s+import\s+.+)$', re.MULTILINE),
        re.compile(r'^(import\s+\S+.*)$', re.MULTILINE),
    ],
    'javascript': [
        re.compile(r'^(import\s+.+from\s+[\'"].+[\'"];?)$', re.MULTILINE),
        re.compile(r'^(const\s+\{.+\}\s*=\s*require\([\'"].+[\'"]\);?)$', re.MULTILINE),
    ],
    'typescript': [
        re.compile(r'^(import\s+.+from\s+[\'"].+[\'"];?)$', re.MULTILINE),
    ],
}

# Config patterns (env vars, constants)
CONFIG_PATTERNS = {
    'python': [
        re.compile(r'^([A-Z_]+\s*=\s*os\.environ\.get\(.+\))$', re.MULTILINE),
        re.compile(r'^([A-Z_]+\s*=\s*int\(os\.environ\.get\(.+\)\))$', re.MULTILINE),
        re.compile(r'^([A-Z_]+\s*=\s*Path\(.+\))$', re.MULTILINE),
    ],
    'javascript': [
        re.compile(r'^(const\s+[A-Z_]+\s*=\s*process\.env\..+)$', re.MULTILINE),
    ],
}

# Docker patterns
DOCKER_PATTERNS = {
    'dockerfile': [
        re.compile(r'^(FROM\s+.+)$', re.MULTILINE),
        re.compile(r'^(RUN\s+pip\s+install.+)$', re.MULTILINE),
        re.compile(r'^(EXPOSE\s+\d+)$', re.MULTILINE),
        re.compile(r'^(CMD\s+.+)$', re.MULTILINE),
        re.compile(r'^(HEALTHCHECK\s+.+)$', re.MULTILINE),
    ],
    'docker-compose': [
        re.compile(r'(traefik\.enable=true)', re.MULTILINE),
        re.compile(r'(traefik\.http\.routers\..+\.rule=.+)', re.MULTILINE),
        re.compile(r'(traefik\.http\.routers\..+\.entrypoints=.+)', re.MULTILINE),
    ],
}

# Function/class patterns
FUNCTION_PATTERNS = {
    'python': [
        re.compile(r'^((?:async\s+)?def\s+\w+\([^)]*\)(?:\s*->\s*\S+)?\s*:)$', re.MULTILINE),
        re.compile(r'^(class\s+\w+(?:\([^)]*\))?\s*:)$', re.MULTILINE),
    ],
    'javascript': [
        re.compile(r'^((?:export\s+)?(?:async\s+)?function\s+\w+\([^)]*\)\s*\{?)$', re.MULTILINE),
        re.compile(r'^(const\s+\w+\s*=\s*(?:async\s+)?\([^)]*\)\s*=>)$', re.MULTILINE),
    ],
}


def _fingerprint(code: str) -> str:
    """Create a stable fingerprint for an atom."""
    # Normalize whitespace and lowercase for matching
    normalized = re.sub(r'\s+', ' ', code.strip()).lower()
    return hashlib.sha256(normalized.encode()).hexdigest()[:12]


def _estimate_tokens(text: str) -> int:
    return len(text) // 4


def _classify_section(atom_code: str, language: str) -> str:
    """Determine what section an atom belongs to."""
    lower = atom_code.lower().strip()
    if any(lower.startswith(k) for k in ['import ', 'from ', 'require(', 'const {']):
        if 'import' in lower or 'require' in lower:
            return 'imports'
    if re.match(r'^[A-Z_]+ *=', atom_code.strip()):
        return 'config'
    if any(lower.startswith(k) for k in ['def ', 'async def ', 'function ', 'const ']):
        if 'def ' in lower or 'function ' in lower or '=>' in lower:
            return 'tools'
    if any(lower.startswith(k) for k in ['class ']):
        return 'models'
    if any(lower.startswith(k) for k in ['from ', 'run ', 'expose', 'cmd ', 'healthcheck']):
        return 'infrastructure'
    if 'traefik' in lower:
        return 'routing'
    return 'other'


def extract_atoms(content: str, language: str) -> list[dict]:
    """Extract atoms from file content."""
    atoms = []
    seen = set()
    
    # Gather all patterns for this language
    all_patterns = []
    for pattern_set in [IMPORT_PATTERNS, CONFIG_PATTERNS, DOCKER_PATTERNS, FUNCTION_PATTERNS]:
        if language in pattern_set:
            all_patterns.extend(pattern_set[language])
    
    # Also try generic patterns
    for pattern_set in [DOCKER_PATTERNS]:
        for lang, patterns in pattern_set.items():
            if lang != language:
                all_patterns.extend(patterns)
    
    for pattern in all_patterns:
        for match in pattern.finditer(content):
            code = match.group(1).strip()
            if not code or len(code) < 5:
                continue
            fp = _fingerprint(code)
            if fp in seen:
                continue
            seen.add(fp)
            atoms.append({
                'fingerprint': fp,
                'code': code,
                'language': language,
                'section': _classify_section(code, language),
                'token_count': _estimate_tokens(code)
            })
    
    return atoms


def scan_file(file_path: str, content: str, language: str, project: str | None = None) -> dict:
    """Scan a file, extract atoms, store in catalog, detect molecules.
    
    Returns scan results summary.
    """
    content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
    
    # Check if already scanned
    db = get_db()
    try:
        existing = db.execute(
            "SELECT id FROM scan_log WHERE content_hash = ?", (content_hash,)
        ).fetchone()
        if existing:
            return {"status": "already_scanned", "content_hash": content_hash}
        
        # Extract atoms
        atoms = extract_atoms(content, language)
        
        # Store atoms in catalog
        new_atoms = 0
        updated_atoms = 0
        atom_fps = []
        
        for atom in atoms:
            atom_fps.append(atom['fingerprint'])
            existing_atom = db.execute(
                "SELECT id, occurrence_count, projects_json, files_json FROM atoms WHERE fingerprint = ?",
                (atom['fingerprint'],)
            ).fetchone()
            
            if existing_atom:
                # Update existing atom
                projects = json.loads(existing_atom['projects_json'])
                files = json.loads(existing_atom['files_json'])
                if project and project not in projects:
                    projects.append(project)
                if file_path not in files:
                    files.append(file_path)
                    if len(files) > 50:  # cap file list
                        files = files[-50:]
                
                db.execute(
                    """UPDATE atoms SET occurrence_count = occurrence_count + 1,
                       last_seen = datetime('now'), projects_json = ?, files_json = ?
                       WHERE id = ?""",
                    (json.dumps(projects), json.dumps(files), existing_atom['id'])
                )
                updated_atoms += 1
            else:
                # New atom
                projects = [project] if project else []
                files = [file_path]
                db.execute(
                    """INSERT INTO atoms (fingerprint, name, code, language, section,
                       token_count, projects_json, files_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (atom['fingerprint'], None, atom['code'], atom['language'],
                     atom['section'], atom['token_count'],
                     json.dumps(projects), json.dumps(files))
                )
                new_atoms += 1
        
        # Detect molecule bonds (atoms co-occurring in this file)
        molecules_matched = 0
        if len(atom_fps) >= 2:
            molecules_matched = _detect_molecules(db, atom_fps, language, project)
        
        # Log the scan
        db.execute(
            """INSERT INTO scan_log (file_path, content_hash, atoms_found, molecules_matched, source)
               VALUES (?, ?, ?, ?, ?)""",
            (file_path, content_hash, len(atoms), molecules_matched, 'workspace')
        )
        
        db.commit()
        
        return {
            "status": "scanned",
            "file_path": file_path,
            "language": language,
            "content_hash": content_hash,
            "atoms_extracted": len(atoms),
            "new_atoms": new_atoms,
            "updated_atoms": updated_atoms,
            "molecules_matched": molecules_matched
        }
    
    except Exception as e:
        logger.error(f"Scan error for {file_path}: {e}")
        return {"status": "error", "error": str(e)}
    finally:
        db.close()


def _detect_molecules(db, atom_fps: list[str], language: str, project: str | None) -> int:
    """Check if atom combination matches or creates a molecule."""
    # Sort fingerprints for consistent matching
    sorted_fps = sorted(atom_fps)
    
    # Check existing molecules for overlap
    molecules = db.execute("SELECT * FROM molecules WHERE language = ?", (language,)).fetchall()
    matched = 0
    
    for mol in molecules:
        mol_fps = json.loads(mol['atom_fingerprints_json'])
        overlap = set(mol_fps) & set(sorted_fps)
        if len(overlap) >= len(mol_fps) * 0.7:  # 70% overlap = match
            # Update molecule
            projects = json.loads(mol['projects_json'])
            if project and project not in projects:
                projects.append(project)
            db.execute(
                """UPDATE molecules SET co_occurrence_count = co_occurrence_count + 1,
                   last_seen = datetime('now'), projects_json = ?
                   WHERE id = ?""",
                (json.dumps(projects), mol['id'])
            )
            matched += 1
    
    # If no match and we have 3+ atoms, consider creating a new molecule candidate
    # (only promoted to molecule after 3+ occurrences)
    if matched == 0 and len(sorted_fps) >= 3:
        # Check if this exact combination exists
        combo_key = json.dumps(sorted_fps)
        existing = db.execute(
            "SELECT id FROM molecules WHERE atom_fingerprints_json = ?", (combo_key,)
        ).fetchone()
        if not existing:
            db.execute(
                """INSERT INTO molecules (atom_fingerprints_json, atom_count, language, projects_json)
                   VALUES (?, ?, ?, ?)""",
                (combo_key, len(sorted_fps), language,
                 json.dumps([project] if project else []))
            )
    
    return matched
