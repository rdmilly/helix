"""Scanner Service - AST-based Code Extraction

Phase 2 implementation: Full AST extraction, dual fingerprinting,
template parameterization. Ported from validated test_editor_poc.py.

All atoms stored with epigenetic meta. Fingerprints tagged with fp_version.
"""
import ast
import hashlib
import json
from services import pg_sync
import logging
import re
import uuid
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from config import CURRENT_FP_VERSION
from services.database import get_db
from services.meta import get_meta_service
from services.registry import get_registry_service
from services.diff import get_diff_service

logger = logging.getLogger(__name__)


# ============================================================
# AST Extraction
# ============================================================

def extract_all_from_source(source: str, filepath: str = "<intake>") -> Dict[str, Any]:
    """Extract everything useful from Python source via AST.
    
    Returns dict with functions, classes, imports, constants.
    Each function includes full metadata for atom creation.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        logger.warning(f"SyntaxError parsing {filepath}: {e}")
        return {"functions": [], "classes": [], "imports": [], "constants": [], "filepath": filepath}
    
    lines = source.split("\n")
    result = {"functions": [], "classes": [], "imports": [], "constants": [], "filepath": filepath}
    
    # Extract functions (top-level and methods)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            start = node.lineno - 1
            end = getattr(node, "end_lineno", len(lines))
            if node.decorator_list:
                start = min(d.lineno for d in node.decorator_list) - 1
            
            code = "\n".join(lines[start:end])
            
            # Get parent class name if method
            parent_class = _find_parent_class(tree, node)
            
            result["functions"].append({
                "name": node.name,
                "full_name": f"{parent_class}.{node.name}" if parent_class else node.name,
                "is_async": isinstance(node, ast.AsyncFunctionDef),
                "is_method": parent_class is not None,
                "parent_class": parent_class,
                "params": [a.arg for a in node.args.args if a.arg != "self"],
                "param_count": len([a for a in node.args.args if a.arg != "self"]),
                "has_return_type": node.returns is not None,
                "docstring": ast.get_docstring(node),
                "decorators": [ast.dump(d) for d in node.decorator_list],
                "line_count": end - start,
                "code": code,
                "token_estimate": len(code) // 4,
                "filepath": filepath,
                "project": _project_from_path(filepath),
            })
    
    # Top-level imports
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            start = node.lineno - 1
            end = getattr(node, "end_lineno", node.lineno)
            code = "\n".join(lines[start:end])
            result["imports"].append({
                "code": code,
                "module": getattr(node, "module", None),
                "names": [a.name for a in node.names] if hasattr(node, "names") else [],
            })
    
    # Constants (top-level UPPER_CASE assignments)
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.isupper():
                    start = node.lineno - 1
                    end = getattr(node, "end_lineno", node.lineno)
                    code = "\n".join(lines[start:end])
                    result["constants"].append({
                        "name": target.id,
                        "code": code,
                    })
    
    return result


def _find_parent_class(tree: ast.AST, target_node: ast.AST) -> Optional[str]:
    """Find the parent class of a function node."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for child in ast.walk(node):
                if child is target_node:
                    return node.name
    return None


def _project_from_path(filepath: str) -> str:
    """Extract project name from file path."""
    parts = Path(filepath).parts
    for i, part in enumerate(parts):
        if part in ("projects", "stacks"):
            if i + 1 < len(parts):
                return parts[i + 1]
    return "unknown"


# ============================================================
# Fingerprinting — dual layer (structural + semantic)
# ============================================================

def structural_fingerprint(func: Dict[str, Any]) -> str:
    """Fingerprint based on shape: async, params, size, decorators.
    
    Tagged with CURRENT_FP_VERSION. Only compare within same version.
    """
    features = [
        "async" if func["is_async"] else "sync",
        "method" if func["is_method"] else "func",
        f"p{func['param_count']}",
        f"ret:{'y' if func['has_return_type'] else 'n'}",
        f"doc:{'y' if func['docstring'] else 'n'}",
        f"sz:{_size_bucket(func['line_count'])}",
        f"dec:{len(func.get('decorators', []))}",
    ]
    return hashlib.sha256("|".join(features).encode()).hexdigest()[:12]


def semantic_fingerprint(func: Dict[str, Any]) -> Tuple[str, List[str]]:
    """Fingerprint based on what the function DOES (API patterns).
    
    Returns (fingerprint_hash, list_of_semantic_tags).
    """
    code = func["code"].lower()
    features = []
    
    # Detect common patterns
    if "@router." in code or "@app." in code:
        if ".get(" in code: features.append("GET_endpoint")
        elif ".post(" in code: features.append("POST_endpoint")
        elif ".delete(" in code: features.append("DELETE_endpoint")
        elif ".put(" in code: features.append("PUT_endpoint")
        else: features.append("endpoint")
    
    if "sqlite3.connect" in code or "get_db" in code: features.append("db_access")
    if "httpexception" in code: features.append("error_handling")
    if "async with httpx" in code or "aiohttp" in code: features.append("http_client")
    if "json.loads" in code or "json.dumps" in code: features.append("json_io")
    if "path(" in code or "mkdir" in code: features.append("filesystem")
    if "logger." in code or "logging." in code: features.append("logging")
    if "try:" in code: features.append("try_catch")
    if "os.environ" in code or "os.getenv" in code: features.append("env_config")
    if "return {" in code or "return json" in code: features.append("json_response")
    if "pydantic" in code or "basemodel" in code: features.append("pydantic")
    if "async def" in code and "await" in code: features.append("async_await")
    if "chromadb" in code or "embedding" in code: features.append("vector_ops")
    if "subprocess" in code or "os.system" in code: features.append("shell_exec")
    if "open(" in code and ("read" in code or "write" in code): features.append("file_io")
    
    if not features:
        features.append("generic")
    
    fp = hashlib.sha256("|".join(sorted(features)).encode()).hexdigest()[:12]
    return fp, features


def _size_bucket(lines: int) -> str:
    if lines <= 5: return "xs"
    if lines <= 15: return "sm"
    if lines <= 40: return "md"
    if lines <= 100: return "lg"
    return "xl"


# ============================================================
# Template Parameterization
# ============================================================

def parameterize_template(func: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Create a parameterized template from function code.
    
    Extracts route paths, env vars, ports into replaceable params.
    Returns (template_string, parameters_dict).
    """
    template = func["code"]
    params = {}
    
    # Extract route paths
    route_matches = re.findall(r'["\'](/[a-z/_{}]+)["\']', template)
    for path in route_matches:
        template = template.replace(f'"{path}"', '"{{route_path}}"', 1)
        template = template.replace(f"'{path}'", "'{{route_path}}'", 1)
        params["route_path"] = path
        break  # Only first route path
    
    # Extract env var names
    env_matches = re.findall(r'os\.environ\.get\(["\'](\w+)["\']', template)
    for var in env_matches:
        params[f"env_{var.lower()}"] = var
    
    # Extract numeric ports
    port_matches = re.findall(r'\b((?:80|443|[3-9]\d{3}|[1-5]\d{4}))\b', template)
    for port in set(port_matches):
        params["port"] = port
        break
    
    # Extract string literals that look like config (URLs, hosts)
    url_matches = re.findall(r'["\']((https?://|localhost)[^\s"\']+)["\']', template)
    for url_match in url_matches:
        url = url_match[0]
        params["url"] = url
        break
    
    return template, params


# ============================================================
# Categorization
# ============================================================

def categorize_function(func: Dict[str, Any], sem_tags: List[str]) -> str:
    """Auto-categorize based on semantic tags and naming conventions."""
    name = func["name"].lower()
    
    if any(t.endswith("_endpoint") for t in sem_tags): return "endpoint"
    if name.startswith(("get_", "list_", "fetch_")): return "query"
    if name.startswith(("create_", "add_", "insert_", "register_")): return "mutation"
    if name.startswith(("delete_", "remove_")): return "deletion"
    if name.startswith(("update_", "save_", "set_")): return "update"
    if name.startswith(("_init", "init_", "setup_")): return "initialization"
    if name.startswith("_"): return "helper"
    if name.startswith(("check_", "validate_", "is_", "has_")): return "validation"
    if "health" in name: return "healthcheck"
    if "db_access" in sem_tags or "database" in name: return "database"
    if "test" in name: return "test"
    return "general"


# ============================================================
# Scanner Service (orchestrates extraction → storage)
# ============================================================

class ScannerService:
    """AST-based code scanner — extracts atoms with full epigenetic metadata."""
    
    def __init__(self):
        self.db = get_db()
        self.meta_service = get_meta_service()
        self.diff_service = get_diff_service()
    
    async def extract_atoms(self, code: str, language: str = "python", filepath: str = "<intake>") -> List[Dict[str, Any]]:
        """Extract atoms from code, compute fingerprints, store with meta.
        
        Returns list of atom records created or updated.
        """
        if language != "python":
            logger.info(f"Scanner: language {language} not yet supported, skipping")
            return []
        
        extraction = extract_all_from_source(code, filepath)
        atoms_created = []
        
        for func in extraction["functions"]:
            if func["line_count"] < 3:
                continue  # Skip trivial functions
            
            try:
                atom = await self._process_function(func)
                if atom:
                    atoms_created.append(atom)
            except Exception as e:
                logger.error(f"Failed to process function {func['name']}: {e}")
                continue
        
        logger.info(f"Scanner extracted {len(atoms_created)} atoms from {filepath}")
        return atoms_created
    
    async def _process_function(self, func: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single function into an atom with full metadata."""
        # Compute fingerprints
        sfp = structural_fingerprint(func)
        sem_fp, sem_tags = semantic_fingerprint(func)
        category = categorize_function(func, sem_tags)
        
        # Parameterize template
        template, params = parameterize_template(func)
        
        # Check for existing atom (name + structural fingerprint + fp_version)
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, occurrence_count, meta FROM atoms WHERE name = ? AND structural_fp = ? AND fp_version = ?",
                (func["name"], sfp, CURRENT_FP_VERSION)
            )
            existing = cursor.fetchone()
            
            if existing:
                # Update occurrence count and meta
                atom_id = existing[0]
                current_meta = pg_sync.dejson(existing[2] or "{}")
                
                # Track project/file appearances in meta
                projects = current_meta.get("provenance", {}).get("projects", [])
                files = current_meta.get("provenance", {}).get("files", [])
                if func["project"] not in projects:
                    projects.append(func["project"])
                if func["filepath"] not in files:
                    files.append(func["filepath"])
                
                # Capture old code before update for diff
                old_code = conn.execute(
                    "SELECT code, template FROM atoms WHERE id = ?", (atom_id,)
                ).fetchone()
                old_code_str = old_code[0] if old_code else ""
                old_template_str = old_code[1] if old_code else ""

                cursor.execute(
                    "UPDATE atoms SET occurrence_count = occurrence_count + 1, last_seen = NOW() WHERE id = ?",
                    (atom_id,)
                )
                conn.commit()

                # Capture diff if code actually changed
                new_code_str = func.get("code", "")
                if new_code_str and new_code_str != old_code_str:
                    self.diff_service.compute_and_store_atom_diff(
                        atom_id,
                        old_code=old_code_str,
                        new_code=new_code_str,
                        old_template=old_template_str or None,
                        new_template=template or None,
                        written_by="scanner_v1",
                    )
                else:
                    # Unchanged — record usage to build maturity
                    self.diff_service.record_usage(atom_id)
                
                # Enrich meta with provenance
                self.meta_service.write_meta("atoms", atom_id, "provenance", {
                    "projects": projects,
                    "files": files,
                    "last_seen_project": func["project"],
                }, written_by="scanner_v1")
                
                logger.debug(f"Updated atom {atom_id} ({func['name']}) occurrence")
                return {"id": atom_id, "name": func["name"], "action": "updated"}
            
            else:
                # Create new atom
                atom_id = f"atom_{uuid.uuid4().hex[:12]}"
                
                cursor.execute("""
                    INSERT INTO atoms (id, name, full_name, code, template, parameters_json,
                        structural_fp, semantic_fp, fp_version, occurrence_count, meta)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, '{}')
                """, (
                    atom_id,
                    func["name"],
                    func.get("full_name"),
                    func["code"],
                    template,
                    json.dumps(params),
                    sfp,
                    sem_fp,
                    CURRENT_FP_VERSION,
                ))
                conn.commit()
                
                # Write structural meta namespace
                self.meta_service.write_meta("atoms", atom_id, "structural", {
                    "language": "python",
                    "is_async": func["is_async"],
                    "is_method": func["is_method"],
                    "parent_class": func["parent_class"],
                    "param_count": func["param_count"],
                    "has_return_type": func["has_return_type"],
                    "has_docstring": bool(func["docstring"]),
                    "line_count": func["line_count"],
                    "token_estimate": func["token_estimate"],
                    "size_bucket": _size_bucket(func["line_count"]),
                    "decorator_count": len(func.get("decorators", [])),
                }, written_by="scanner_v1")
                
                # Write semantic meta namespace
                self.meta_service.write_meta("atoms", atom_id, "semantic", {
                    "tags": sem_tags,
                    "category": category,
                }, written_by="scanner_v1")
                
                # Write provenance meta namespace
                self.meta_service.write_meta("atoms", atom_id, "provenance", {
                    "projects": [func["project"]],
                    "files": [func["filepath"]],
                    "first_seen_project": func["project"],
                }, written_by="scanner_v1")
                
                logger.info(f"Created atom {atom_id}: {func['name']} ({category})")

                # Publish pattern.detected for bus subscribers (molecule_assembler, dictionary_updater)
                try:
                    from services.event_bus import publish as _pub
                    _pub("pattern.detected", {
                        "atom_id": atom_id,
                        "atom_name": func["name"],
                        "category": category,
                        "structural_fp": sfp,
                        "semantic_tags": sem_tags,
                        "project": func.get("project"),
                        "filepath": func.get("filepath"),
                    })
                except Exception:
                    pass

                return {
                    "id": atom_id,
                    "name": func["name"],
                    "full_name": func.get("full_name"),
                    "category": category,
                    "structural_fp": sfp,
                    "semantic_fp": sem_fp,
                    "semantic_tags": sem_tags,
                    "line_count": func["line_count"],
                    "token_estimate": func["token_estimate"],
                    "action": "created",
                }
    
    async def compute_structural_fingerprint(self, code: str, language: str = "python") -> str:
        """Compute structural fingerprint for a code snippet."""
        extraction = extract_all_from_source(code)
        if not extraction["functions"]:
            return hashlib.sha256(code.encode()).hexdigest()[:12]
        return structural_fingerprint(extraction["functions"][0])
    
    async def compute_semantic_fingerprint(self, code: str, language: str = "python") -> Tuple[str, List[str]]:
        """Compute semantic fingerprint for a code snippet."""
        extraction = extract_all_from_source(code)
        if not extraction["functions"]:
            return hashlib.sha256(code.encode()).hexdigest()[:12], ["generic"]
        return semantic_fingerprint(extraction["functions"][0])


# Global service instance
scanner_service = ScannerService()


def get_scanner_service() -> ScannerService:
    """Get scanner service instance"""
    return scanner_service
