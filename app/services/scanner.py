"""Scanner Service — Self-Expanding Language-Agnostic Code Scanner

Three-tier architecture (spec: helix-scanner-self-expanding.md):
  Tier 1: Tree-sitter universal parsing (100+ languages, near-zero cost)
  Tier 2: LLM-powered structural analysis (Haiku, ~$0.001/file)
  Tier 3: Self-generated heuristics (learned from Tier 2, near-zero cost)

Preserves: dual fingerprinting, template parameterization, epigenetic meta.
All atoms stored with scanner_tier meta namespace tracking which tier analyzed them.
"""
import ast
import hashlib
import json
import logging
import re
import uuid
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from config import (
    CURRENT_FP_VERSION, HAIKU_MODEL,
    GRAMMAR_DIR, HEURISTIC_GENERATION_THRESHOLD,
)
from services.database import get_db
from services.meta import get_meta_service
from services.registry import get_registry_service

logger = logging.getLogger(__name__)


# ============================================================
# Language Detection
# ============================================================

# Map file extensions → tree-sitter language names
EXTENSION_MAP = {
    ".py": "python", ".pyw": "python",
    ".js": "javascript", ".mjs": "javascript", ".cjs": "javascript",
    ".ts": "typescript", ".tsx": "tsx",
    ".jsx": "javascript",
    ".rs": "rust",
    ".go": "go",
    ".rb": "ruby",
    ".c": "c", ".h": "c",
    ".cpp": "cpp", ".cc": "cpp", ".cxx": "cpp", ".hpp": "cpp",
    ".java": "java",
    ".kt": "kotlin", ".kts": "kotlin",
    ".swift": "swift",
    ".php": "php",
    ".lua": "lua",
    ".zig": "zig",
    ".hs": "haskell",
    ".sh": "bash", ".bash": "bash", ".zsh": "bash",
    ".css": "css",
    ".html": "html", ".htm": "html",
    ".json": "json",
    ".yaml": "yaml", ".yml": "yaml",
    ".toml": "toml",
    ".sql": "sql",
    ".r": "r", ".R": "r",
    ".scala": "scala",
    ".ex": "elixir", ".exs": "elixir",
    ".erl": "erlang",
    ".cs": "c_sharp",
    ".dart": "dart",
    ".jl": "julia",
    ".ml": "ocaml",
    ".vim": "vim",
    ".dockerfile": "dockerfile",
    ".tf": "hcl",
    ".nix": "nix",
}


def detect_language(filename: str, language_hint: Optional[str] = None) -> str:
    """Detect language from filename extension or explicit hint."""
    if language_hint:
        return language_hint.lower()
    
    ext = Path(filename).suffix.lower()
    
    # Special cases without extensions
    basename = Path(filename).name.lower()
    if basename == "dockerfile":
        return "dockerfile"
    if basename == "makefile":
        return "make"
    if basename in ("jenkinsfile", "vagrantfile"):
        return "ruby"  # Close enough for structure
    
    return EXTENSION_MAP.get(ext, "unknown")


# ============================================================
# Tier 1: Tree-sitter Universal Parsing
# ============================================================

_ts_languages = {}  # Cache: lang_name → Language object


def _get_tree_sitter_language(lang: str):
    """Get a tree-sitter Language object, auto-loading grammar.
    
    Uses tree_sitter_languages package for pre-built grammars.
    Returns None if language not available.
    """
    if lang in _ts_languages:
        return _ts_languages[lang]
    
    try:
        from tree_sitter_languages import get_language, get_parser
        language = get_language(lang)
        _ts_languages[lang] = language
        return language
    except (ImportError, Exception) as e:
        logger.debug(f"Tree-sitter language '{lang}' not available: {e}")
        _ts_languages[lang] = None
        return None


def tree_sitter_parse(code: str, lang: str) -> Optional[Dict[str, Any]]:
    """Parse code using tree-sitter and extract function/block boundaries.
    
    Returns extraction dict compatible with atom creation pipeline,
    or None if parsing fails.
    """
    language = _get_tree_sitter_language(lang)
    if language is None:
        return None
    
    try:
        from tree_sitter_languages import get_parser
        parser = get_parser(lang)
        tree = parser.parse(code.encode("utf-8"))
        root = tree.root_node
    except Exception as e:
        logger.warning(f"Tree-sitter parse failed for {lang}: {e}")
        return None
    
    functions = []
    lines = code.split("\n")
    
    # Walk AST to find function/method definitions
    _walk_for_functions(root, lines, lang, functions, code)
    
    return {
        "functions": functions,
        "classes": [],
        "imports": [],
        "constants": [],
        "language": lang,
        "parser": "tree-sitter",
    }


def _walk_for_functions(
    node, lines: List[str], lang: str,
    functions: List[Dict], source: str,
    parent_class: Optional[str] = None
):
    """Recursively walk tree-sitter AST to extract functions.
    
    Handles language-agnostic function-like node types.
    """
    # Node types that represent function definitions across languages
    FUNCTION_TYPES = {
        "function_definition",       # Python, Lua
        "function_declaration",      # JS, TS, C, Go, PHP
        "method_definition",         # Ruby, Python
        "method_declaration",        # Java, Kotlin, C#
        "function_item",             # Rust
        "func_literal",              # Go anonymous
        "arrow_function",            # JS/TS
        "lambda",                    # Python
        "function_expression",       # JS
        "generator_function_declaration",  # JS
        "async_function_declaration",  # JS async
    }
    
    CLASS_TYPES = {
        "class_definition",          # Python
        "class_declaration",         # JS, TS, Java, C#
        "class_specifier",           # C++
        "struct_item",               # Rust
        "impl_item",                 # Rust impl blocks
    }
    
    # Check if this node is a class — track for method extraction
    current_class = parent_class
    if node.type in CLASS_TYPES:
        # Try to extract class name from first identifier child
        for child in node.children:
            if child.type in ("identifier", "type_identifier", "name"):
                current_class = child.text.decode("utf-8")
                break
    
    # Check if this node is a function
    if node.type in FUNCTION_TYPES:
        func = _extract_function_from_node(node, lines, lang, source, current_class)
        if func and func.get("line_count", 0) >= 3:
            functions.append(func)
    
    # Recurse into children
    for child in node.children:
        _walk_for_functions(child, lines, lang, functions, source, current_class)


def _extract_function_from_node(
    node, lines: List[str], lang: str, source: str,
    parent_class: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Extract function metadata from a tree-sitter node."""
    start_line = node.start_point[0]
    end_line = node.end_point[0] + 1
    
    # Extract function name
    name = None
    params = []
    is_async = False
    has_return_type = False
    
    for child in node.children:
        if child.type in ("identifier", "property_identifier", "name"):
            if name is None:
                name = child.text.decode("utf-8")
        
        elif child.type in ("formal_parameters", "parameters", "parameter_list"):
            # Extract parameter names
            for param_node in child.children:
                if param_node.type in ("identifier", "simple_parameter",
                                        "required_parameter", "optional_parameter",
                                        "typed_parameter", "parameter"):
                    # Try to get just the name
                    param_text = param_node.text.decode("utf-8")
                    # Strip type annotations, defaults, etc.
                    param_name = param_text.split(":")[0].split("=")[0].strip()
                    if param_name and param_name not in ("self", "cls", ",", "(", ")"):
                        params.append(param_name)
        
        elif child.type in ("return_type", "type_annotation"):
            has_return_type = True
    
    # Check async
    if "async" in node.type or any(
        c.type == "async" for c in node.children
    ):
        is_async = True
    
    if not name:
        # Anonymous function — generate name from position
        name = f"_anon_{start_line}"
    
    code = "\n".join(lines[start_line:end_line])
    
    # Try to extract docstring (first string literal in body)
    docstring = None
    body_node = None
    for child in node.children:
        if child.type in ("block", "body", "statement_block", "compound_statement"):
            body_node = child
            break
    
    if body_node and body_node.children:
        first_stmt = body_node.children[0] if body_node.children else None
        if first_stmt and first_stmt.type in ("expression_statement", "string"):
            doc_text = first_stmt.text.decode("utf-8").strip()
            if doc_text.startswith(('"""', "'''", '"', "'")):
                docstring = doc_text.strip("\"'").strip()
    
    return {
        "name": name,
        "full_name": f"{parent_class}.{name}" if parent_class else name,
        "is_async": is_async,
        "is_method": parent_class is not None,
        "parent_class": parent_class,
        "params": params,
        "param_count": len(params),
        "has_return_type": has_return_type,
        "docstring": docstring,
        "decorators": [],
        "line_count": end_line - start_line,
        "code": code,
        "token_estimate": len(code) // 4,
        "filepath": "<tree-sitter>",
        "project": "unknown",
    }


# ============================================================
# Tier 2: LLM-Powered Structural Analysis
# ============================================================

STRUCTURAL_ANALYSIS_PROMPT = """Extract all function/block boundaries from this code.
Language: {language}

Return ONLY valid JSON with this structure:
{{
  "functions": [
    {{
      "name": "function_name",
      "full_name": "ClassName.method_name or just function_name",
      "is_async": true/false,
      "is_method": true/false,
      "parent_class": "ClassName" or null,
      "params": ["param1", "param2"],
      "param_count": 2,
      "has_return_type": true/false,
      "docstring": "docstring text" or null,
      "start_line": 1,
      "end_line": 20,
      "line_count": 20
    }}
  ],
  "imports": ["module1", "module2"],
  "classes": ["ClassName"]
}}

Code:
```
{code}
```"""


async def llm_structural_analysis(
    code: str, lang: str, filepath: str = "<llm-analysis>"
) -> Optional[Dict[str, Any]]:
    """Use Haiku to analyze code structure for languages without parsers.
    
    Returns extraction dict compatible with atom creation pipeline.
    """
    from services.haiku import get_haiku_service
    haiku = get_haiku_service()
    
    system = (
        "You are a precise code structure analyzer. Extract function boundaries, "
        "parameters, imports, and class definitions. Return ONLY valid JSON. "
        "No markdown, no preamble."
    )
    
    prompt = STRUCTURAL_ANALYSIS_PROMPT.format(
        language=lang,
        code=code[:8000]  # Cap at ~8K chars to stay within token limits
    )
    
    result = await haiku._call_api(system, prompt, max_tokens=2048)
    
    if not result:
        logger.warning(f"LLM structural analysis returned no result for {lang}")
        return None
    
    try:
        # Parse JSON response
        from services.parser import extract_json
        parsed = extract_json(result)
        
        if not parsed or not isinstance(parsed, dict):
            logger.warning(f"LLM structural analysis returned invalid JSON: {result[:200]}")
            return None
        
        lines = code.split("\n")
        functions = []
        
        for func_data in parsed.get("functions", []):
            start = func_data.get("start_line", 1) - 1
            end = func_data.get("end_line", start + func_data.get("line_count", 1))
            func_code = "\n".join(lines[max(0, start):min(end, len(lines))])
            
            functions.append({
                "name": func_data.get("name", "_unknown"),
                "full_name": func_data.get("full_name", func_data.get("name", "_unknown")),
                "is_async": func_data.get("is_async", False),
                "is_method": func_data.get("is_method", False),
                "parent_class": func_data.get("parent_class"),
                "params": func_data.get("params", []),
                "param_count": func_data.get("param_count", len(func_data.get("params", []))),
                "has_return_type": func_data.get("has_return_type", False),
                "docstring": func_data.get("docstring"),
                "decorators": [],
                "line_count": func_data.get("line_count", end - start),
                "code": func_code,
                "token_estimate": len(func_code) // 4,
                "filepath": filepath,
                "project": _project_from_path(filepath),
            })
        
        return {
            "functions": functions,
            "classes": parsed.get("classes", []),
            "imports": parsed.get("imports", []),
            "constants": [],
            "language": lang,
            "parser": "llm-structural",
        }
    
    except Exception as e:
        logger.error(f"LLM structural analysis parsing failed: {e}")
        return None


# ============================================================
# Tier 3: Self-Generated Heuristics
# ============================================================

def heuristic_parse(code: str, lang: str) -> Optional[Dict[str, Any]]:
    """Parse code using learned heuristic rules from language_heuristics table.
    
    Returns extraction dict, or None if no heuristics exist for this language.
    """
    db = get_db()
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT rules_json, accuracy_score FROM language_heuristics WHERE language = ?",
            (lang,)
        )
        row = cursor.fetchone()
    
    if not row:
        return None
    
    try:
        rules = json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        logger.warning(f"Invalid heuristic rules for {lang}")
        return None
    
    lines = code.split("\n")
    functions = []
    
    # Apply regex-based function boundary detection
    func_pattern = rules.get("function_pattern")
    if func_pattern:
        try:
            regex = re.compile(func_pattern, re.MULTILINE)
            for match in regex.finditer(code):
                start_line = code[:match.start()].count("\n")
                # Find end of function (next match or end of file)
                name = match.group(1) if match.lastindex and match.lastindex >= 1 else f"_heuristic_{start_line}"
                
                # Estimate end line using indentation heuristics or next function
                end_line = _estimate_function_end(lines, start_line, rules)
                func_code = "\n".join(lines[start_line:end_line])
                
                # Extract params if pattern provided
                params = []
                param_pattern = rules.get("param_pattern")
                if param_pattern:
                    param_match = re.search(param_pattern, func_code)
                    if param_match:
                        param_text = param_match.group(1) if param_match.lastindex else ""
                        params = [p.strip().split(":")[0].split("=")[0].strip()
                                  for p in param_text.split(",") if p.strip()]
                
                functions.append({
                    "name": name,
                    "full_name": name,
                    "is_async": bool(rules.get("async_pattern") and
                                     re.search(rules["async_pattern"], func_code)),
                    "is_method": False,
                    "parent_class": None,
                    "params": params,
                    "param_count": len(params),
                    "has_return_type": bool(rules.get("return_type_pattern") and
                                            re.search(rules["return_type_pattern"], func_code)),
                    "docstring": None,
                    "decorators": [],
                    "line_count": end_line - start_line,
                    "code": func_code,
                    "token_estimate": len(func_code) // 4,
                    "filepath": "<heuristic>",
                    "project": "unknown",
                })
        except re.error as e:
            logger.error(f"Invalid heuristic regex for {lang}: {e}")
            return None
    
    if not functions:
        return None
    
    return {
        "functions": functions,
        "classes": [],
        "imports": [],
        "constants": [],
        "language": lang,
        "parser": "learned-heuristic",
    }


def _estimate_function_end(lines: List[str], start_line: int, rules: Dict) -> int:
    """Estimate where a function ends using indentation or rules."""
    end_pattern = rules.get("block_end_pattern")
    
    if end_pattern:
        # Use explicit block end pattern (e.g., "^}" for C-like languages)
        try:
            regex = re.compile(end_pattern)
            brace_depth = 0
            for i in range(start_line, len(lines)):
                line = lines[i]
                brace_depth += line.count("{") - line.count("}")
                if i > start_line and brace_depth <= 0 and regex.search(line):
                    return i + 1
        except re.error:
            pass
    
    # Fallback: indentation-based (works for Python-like)
    if start_line + 1 < len(lines):
        base_indent = len(lines[start_line]) - len(lines[start_line].lstrip())
        for i in range(start_line + 1, len(lines)):
            stripped = lines[i].strip()
            if not stripped:
                continue
            current_indent = len(lines[i]) - len(lines[i].lstrip())
            if current_indent <= base_indent and stripped:
                return i
    
    return min(start_line + 50, len(lines))  # Cap at 50 lines


async def _schedule_heuristic_generation(lang: str):
    """Check if enough LLM analyses exist to generate heuristics for a language.
    
    When threshold is met, asks Haiku to generate regex/heuristic rules.
    """
    db = get_db()
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        
        # Count atoms analyzed by LLM for this language
        cursor.execute("""
            SELECT COUNT(*) FROM atoms a
            JOIN json_extract(a.meta, '$.scanner_tier.tier') as tier
            WHERE tier = 'llm-structural'
            AND json_extract(a.meta, '$.structural.language') = ?
        """, (lang,))
        
        # Simpler fallback query if JSON extract fails
        count = 0
        try:
            row = cursor.fetchone()
            count = row[0] if row else 0
        except Exception:
            # Fallback: scan meta_events for scanner_tier writes
            cursor.execute("""
                SELECT COUNT(DISTINCT target_id) FROM meta_events
                WHERE namespace = 'scanner_tier'
                AND new_value LIKE '%llm-structural%'
                AND new_value LIKE ?
            """, (f'%{lang}%',))
            row = cursor.fetchone()
            count = row[0] if row else 0
    
    if count < HEURISTIC_GENERATION_THRESHOLD:
        logger.debug(f"Language {lang}: {count}/{HEURISTIC_GENERATION_THRESHOLD} LLM analyses, not ready for heuristics")
        return
    
    logger.info(f"Language {lang}: {count} LLM analyses, generating heuristics...")
    
    # Collect sample analyses to send to Haiku
    # (This would gather the structural analyses and ask Haiku to generate rules)
    # For now, log the intent — full implementation in Phase 5
    logger.info(f"HEURISTIC GENERATION READY for {lang} — queued for next processing cycle")
    
    # TODO Phase 5: Collect N sample function extractions for this language,
    # send to Haiku with prompt to generate regex rules,
    # store result in language_heuristics table


# ============================================================
# Legacy: Python AST Extraction (kept as optimized Tier 1 fallback)
# ============================================================

def extract_all_from_source(source: str, filepath: str = "<intake>") -> Dict[str, Any]:
    """Extract everything useful from Python source via AST.
    
    Returns dict with functions, classes, imports, constants.
    Each function includes full metadata for atom creation.
    
    NOTE: This is the original Python-only parser, kept as a fast path
    for Python when tree-sitter is unavailable.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        logger.warning(f"SyntaxError parsing {filepath}: {e}")
        return {"functions": [], "classes": [], "imports": [], "constants": [], "filepath": filepath}
    
    lines = source.split("\n")
    result = {"functions": [], "classes": [], "imports": [], "constants": [], "filepath": filepath}
    
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            start = node.lineno - 1
            end = getattr(node, "end_lineno", len(lines))
            if node.decorator_list:
                start = min(d.lineno for d in node.decorator_list) - 1
            
            code = "\n".join(lines[start:end])
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
    
    route_matches = re.findall(r'["\'](/[a-z/_{}]+)["\']', template)
    for path in route_matches:
        template = template.replace(f'"{path}"', '"{{route_path}}"', 1)
        template = template.replace(f"'{path}'", "'{{route_path}}'", 1)
        params["route_path"] = path
        break
    
    env_matches = re.findall(r'os\.environ\.get\(["\'](\w+)["\']', template)
    for var in env_matches:
        params[f"env_{var.lower()}"] = var
    
    port_matches = re.findall(r'\b((?:80|443|[3-9]\d{3}|[1-5]\d{4}))\b', template)
    for port in set(port_matches):
        params["port"] = port
        break
    
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
# Scanner Service (three-tier resolution)
# ============================================================

class ScannerService:
    """Self-expanding language-agnostic code scanner.
    
    Three-tier resolution:
      1. Tree-sitter (fast, free, 100+ languages)
      2. Learned heuristics (fast, free, auto-generated from Tier 2)
      3. LLM structural analysis (Haiku API call, ~$0.001/file)
    
    All atoms stored with scanner_tier meta namespace.
    """
    
    def __init__(self):
        self.db = get_db()
        self.meta_service = get_meta_service()
    
    async def scan_code(
        self, code: str, filename: str,
        language_hint: Optional[str] = None,
        filepath: Optional[str] = None
    ) -> Dict[str, Any]:
        """Three-tier scan resolution. Returns extraction result with tier metadata.
        
        This is the main entry point — replaces the old language-specific approach.
        """
        lang = detect_language(filename, language_hint)
        effective_filepath = filepath or filename
        scanner_tier_meta = {}
        
        # Tier 1: Tree-sitter
        result = tree_sitter_parse(code, lang)
        if result and result.get("functions"):
            scanner_tier_meta = {
                "tier": "tree-sitter",
                "grammar": lang,
                "model": None,
                "heuristic_version": None,
                "parse_confidence": 0.95,
            }
            result["scanner_tier"] = scanner_tier_meta
            logger.info(f"Tier 1 (tree-sitter/{lang}): extracted {len(result['functions'])} functions from {filename}")
            return result
        
        # Tier 1 fallback: Python AST for .py files (even if tree-sitter unavailable)
        if lang == "python":
            result = extract_all_from_source(code, effective_filepath)
            if result.get("functions"):
                scanner_tier_meta = {
                    "tier": "tree-sitter",  # Logically equivalent
                    "grammar": "python-ast",
                    "model": None,
                    "heuristic_version": None,
                    "parse_confidence": 0.98,
                }
                result["scanner_tier"] = scanner_tier_meta
                result["language"] = "python"
                result["parser"] = "python-ast"
                logger.info(f"Tier 1 (python-ast): extracted {len(result['functions'])} functions from {filename}")
                return result
        
        # Tier 3: Learned heuristics (checked before Tier 2 — cheaper)
        result = heuristic_parse(code, lang)
        if result and result.get("functions"):
            scanner_tier_meta = {
                "tier": "learned-heuristic",
                "grammar": None,
                "model": None,
                "heuristic_version": lang,
                "parse_confidence": 0.80,
            }
            result["scanner_tier"] = scanner_tier_meta
            logger.info(f"Tier 3 (heuristic/{lang}): extracted {len(result['functions'])} functions from {filename}")
            return result
        
        # Tier 2: LLM structural analysis
        result = await llm_structural_analysis(code, lang, effective_filepath)
        if result and result.get("functions"):
            scanner_tier_meta = {
                "tier": "llm-structural",
                "grammar": None,
                "model": HAIKU_MODEL,
                "heuristic_version": None,
                "parse_confidence": 0.85,
            }
            result["scanner_tier"] = scanner_tier_meta
            logger.info(f"Tier 2 (llm/{lang}): extracted {len(result['functions'])} functions from {filename}")
            
            # Check if we should promote to Tier 3
            await _schedule_heuristic_generation(lang)
            
            return result
        
        # No tier could parse this
        logger.warning(f"All scanner tiers failed for {filename} (language: {lang})")
        return {
            "functions": [],
            "classes": [],
            "imports": [],
            "constants": [],
            "language": lang,
            "parser": "none",
            "scanner_tier": {
                "tier": "failed",
                "grammar": None,
                "model": None,
                "heuristic_version": None,
                "parse_confidence": 0.0,
            }
        }
    
    async def extract_atoms(
        self, code: str, language: str = "python",
        filepath: str = "<intake>", filename: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Extract atoms from code using three-tier scanner.
        
        Backward-compatible API — existing callers can use this unchanged.
        Returns list of atom records created or updated.
        """
        # Use new three-tier scan
        effective_filename = filename or filepath
        extraction = await self.scan_code(code, effective_filename, language_hint=language, filepath=filepath)
        
        scanner_tier = extraction.get("scanner_tier", {})
        atoms_created = []
        
        for func in extraction.get("functions", []):
            if func.get("line_count", 0) < 3:
                continue
            
            # Enrich with filepath/project from the intake call
            func["filepath"] = filepath
            func["project"] = _project_from_path(filepath)
            
            try:
                atom = await self._process_function(func, scanner_tier)
                if atom:
                    atoms_created.append(atom)
            except Exception as e:
                logger.error(f"Failed to process function {func.get('name', '?')}: {e}")
                continue
        
        logger.info(
            f"Scanner extracted {len(atoms_created)} atoms from {filepath} "
            f"(tier: {scanner_tier.get('tier', 'unknown')}, lang: {extraction.get('language', 'unknown')})"
        )
        return atoms_created
    
    async def _process_function(
        self, func: Dict[str, Any],
        scanner_tier: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Process a single function into an atom with full metadata."""
        sfp = structural_fingerprint(func)
        sem_fp, sem_tags = semantic_fingerprint(func)
        category = categorize_function(func, sem_tags)
        template, params = parameterize_template(func)
        
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, occurrence_count, meta FROM atoms WHERE name = ? AND structural_fp = ? AND fp_version = ?",
                (func["name"], sfp, CURRENT_FP_VERSION)
            )
            existing = cursor.fetchone()
            
            if existing:
                atom_id = existing[0]
                current_meta = json.loads(existing[2] or "{}")
                
                projects = current_meta.get("provenance", {}).get("projects", [])
                files = current_meta.get("provenance", {}).get("files", [])
                if func["project"] not in projects:
                    projects.append(func["project"])
                if func["filepath"] not in files:
                    files.append(func["filepath"])
                
                cursor.execute(
                    "UPDATE atoms SET occurrence_count = occurrence_count + 1, last_seen = datetime('now') WHERE id = ?",
                    (atom_id,)
                )
                conn.commit()
                
                self.meta_service.write_meta("atoms", atom_id, "provenance", {
                    "projects": projects,
                    "files": files,
                    "last_seen_project": func["project"],
                }, written_by="scanner_v2")
                
                # Write scanner_tier meta
                self.meta_service.write_meta("atoms", atom_id, "scanner_tier", scanner_tier,
                                              written_by="scanner_v2")
                
                logger.debug(f"Updated atom {atom_id} ({func['name']}) occurrence")
                return {"id": atom_id, "name": func["name"], "action": "updated"}
            
            else:
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
                
                # Structural meta — now language-agnostic
                self.meta_service.write_meta("atoms", atom_id, "structural", {
                    "language": func.get("language", detect_language(func.get("filepath", ""), None)),
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
                }, written_by="scanner_v2")
                
                # Semantic meta
                self.meta_service.write_meta("atoms", atom_id, "semantic", {
                    "tags": sem_tags,
                    "category": category,
                }, written_by="scanner_v2")
                
                # Provenance meta
                self.meta_service.write_meta("atoms", atom_id, "provenance", {
                    "projects": [func["project"]],
                    "files": [func["filepath"]],
                    "first_seen_project": func["project"],
                }, written_by="scanner_v2")
                
                # Scanner tier meta (new namespace)
                self.meta_service.write_meta("atoms", atom_id, "scanner_tier",
                                              scanner_tier, written_by="scanner_v2")
                
                logger.info(f"Created atom {atom_id}: {func['name']} ({category}) via {scanner_tier.get('tier', 'unknown')}")
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
                    "scanner_tier": scanner_tier.get("tier"),
                    "language": func.get("language", "unknown"),
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
