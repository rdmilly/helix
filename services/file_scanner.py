"""File Scanner Service — Phase 1c

Background worker that walks configured project directories,
hash-indexes every file, and sends new/changed files to the
Forge scan endpoint to populate the atom catalog.

Design:
- Hash state persisted in DATA_DIR/file_scanner_state.json
- Only sends files that are new or have changed (SHA256 hash)
- Skips binary files, .git dirs, __pycache__, node_modules, venvs
- Respects .helix/config.yml scan config if present
- Calls Forge /api/forge/scan for each eligible file
- First run on a fresh system will populate thousands of atoms

Config (.helix/config.yml or defaults):
  scan_paths: ["/opt/projects"]
  scan_extensions: [".py", ".js", ".ts", ".go", ".rs", ".yaml", ".yml", ".json", ".sh"]
  scan_exclude: ["__pycache__", ".git", "node_modules", ".venv", "venv", "dist", "build"]
  max_file_size_kb: 500
  forge_url: http://the-forge:9095
"""
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import httpx

log = logging.getLogger("helix.file_scanner")

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
FORGE_URL = os.environ.get("FORGE_URL", "http://the-forge:9095")
STATE_PATH = DATA_DIR / "file_scanner_state.json"
HELIX_CONFIG_PATH = Path("/opt/projects/.helix/config.yml")

DEFAULT_SCAN_PATHS: List[str] = ["/opt/projects"]
DEFAULT_EXTENSIONS: Set[str] = {
    ".py", ".js", ".ts", ".jsx", ".tsx",
    ".go", ".rs", ".rb", ".sh", ".bash",
    ".yaml", ".yml", ".json", ".toml",
    ".md", ".dockerfile",
}
DEFAULT_EXCLUDES: Set[str] = {
    "__pycache__", ".git", "node_modules",
    ".venv", "venv", "env", ".env",
    "dist", "build", ".mypy_cache",
    ".pytest_cache", "migrations",
    "helix-vps2-archive-20260314",
}
MAX_FILE_SIZE_BYTES = 500 * 1024  # 500KB
FORGE_TIMEOUT = 30.0
BATCH_PAUSE = 0.05  # 50ms between scans


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_config() -> Dict[str, Any]:
    cfg = {
        "scan_paths": DEFAULT_SCAN_PATHS,
        "scan_extensions": list(DEFAULT_EXTENSIONS),
        "scan_exclude": list(DEFAULT_EXCLUDES),
        "max_file_size_kb": 500,
        "forge_url": FORGE_URL,
    }
    if HELIX_CONFIG_PATH.exists():
        try:
            import yaml
            raw = yaml.safe_load(HELIX_CONFIG_PATH.read_text())
            if isinstance(raw, dict):
                scan = raw.get("scan", {})
                if scan.get("paths"): cfg["scan_paths"] = scan["paths"]
                if scan.get("extensions"): cfg["scan_extensions"] = scan["extensions"]
                if scan.get("exclude"): cfg["scan_exclude"] = scan["exclude"]
                if scan.get("max_file_size_kb"): cfg["max_file_size_kb"] = scan["max_file_size_kb"]
            log.info(f"Loaded scan config from {HELIX_CONFIG_PATH}")
        except Exception as e:
            log.warning(f"Could not load .helix/config.yml: {e}, using defaults")
    return cfg


# ---------------------------------------------------------------------------
# Hash state
# ---------------------------------------------------------------------------

def _load_state() -> Dict[str, str]:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:
            pass
    return {}


def _save_state(state: Dict[str, str]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2))


def _file_hash(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


# ---------------------------------------------------------------------------
# File walker
# ---------------------------------------------------------------------------

def _should_skip_dir(name: str, excludes: Set[str]) -> bool:
    return name in excludes or name.startswith(".")


def _iter_files(scan_paths: List[str], extensions: Set[str],
                excludes: Set[str], max_bytes: int):
    for root_str in scan_paths:
        root = Path(root_str)
        if not root.exists():
            log.warning(f"Scan path does not exist: {root}")
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            dp = Path(dirpath)
            dirnames[:] = [d for d in dirnames if not _should_skip_dir(d, excludes)]
            for fname in filenames:
                fpath = dp / fname
                ext = fpath.suffix.lower()
                if fname.lower() == "dockerfile":
                    ext = ".dockerfile"
                if ext not in extensions:
                    continue
                try:
                    if fpath.stat().st_size > max_bytes:
                        continue
                except OSError:
                    continue
                try:
                    rel = fpath.relative_to(root)
                    project = rel.parts[0] if len(rel.parts) > 1 else root.name
                except ValueError:
                    project = root.name
                yield fpath, project


# ---------------------------------------------------------------------------
# Forge caller
# ---------------------------------------------------------------------------

async def _scan_file(client: httpx.AsyncClient, fpath: Path,
                     project: str, forge_url: str) -> Dict[str, Any]:
    ext_to_lang = {
        ".py": "python", ".js": "javascript", ".ts": "typescript",
        ".jsx": "javascript", ".tsx": "typescript", ".go": "go",
        ".rs": "rust", ".rb": "ruby", ".sh": "bash", ".bash": "bash",
        ".yaml": "yaml", ".yml": "yaml", ".json": "json",
        ".toml": "toml", ".md": "markdown", ".dockerfile": "dockerfile",
    }
    try:
        content = fpath.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        return {"status": "read_error", "error": str(e)}

    language = ext_to_lang.get(fpath.suffix.lower())
    try:
        r = await client.post(
            f"{forge_url}/api/forge/scan",
            json={
                "file_path": str(fpath),
                "content": content,
                "language": language,
                "project": project,
                "source": "file-scanner",
            },
            timeout=FORGE_TIMEOUT,
        )
        r.raise_for_status()
        return r.json()
    except httpx.HTTPStatusError as e:
        return {"status": "http_error", "code": e.response.status_code}
    except Exception as e:
        return {"status": "error", "error": str(e)[:100]}


# ---------------------------------------------------------------------------
# Main scan job
# ---------------------------------------------------------------------------

async def run_file_scan() -> Dict[str, Any]:
    """Walk all configured project paths, hash-index files, send new/changed
    ones to Forge for atom extraction. Returns stats dict."""
    import asyncio
    start = time.time()
    cfg = _load_config()
    state = _load_state()
    forge_url = cfg["forge_url"]
    extensions = set(cfg["scan_extensions"])
    excludes = set(cfg["scan_exclude"])
    max_bytes = cfg["max_file_size_kb"] * 1024

    stats: Dict[str, Any] = {
        "scanned": 0, "new": 0, "changed": 0,
        "skipped_unchanged": 0, "errors": 0,
        "atoms_created": 0, "atoms_updated": 0,
        "duration_s": 0.0,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }

    log.info(f"File scanner starting — paths={cfg['scan_paths']}")
    new_state = dict(state)

    async with httpx.AsyncClient() as client:
        for fpath, project in _iter_files(cfg["scan_paths"], extensions, excludes, max_bytes):
            stats["scanned"] += 1
            try:
                current_hash = _file_hash(fpath)
            except Exception:
                stats["errors"] += 1
                continue

            key = str(fpath)
            prev_hash = state.get(key)

            if prev_hash == current_hash:
                stats["skipped_unchanged"] += 1
                continue

            is_new = prev_hash is None
            stats["new" if is_new else "changed"] += 1

            result = await _scan_file(client, fpath, project, forge_url)

            if result.get("status") in ("error", "read_error", "http_error"):
                stats["errors"] += 1
                log.warning(f"Scan failed for {fpath}: {result}")
            else:
                stats["atoms_created"] += result.get("new_atoms", result.get("atoms_created", 0))
                stats["atoms_updated"] += result.get("updated_atoms", result.get("atoms_updated", 0))
                new_state[key] = current_hash

            await asyncio.sleep(BATCH_PAUSE)

            if stats["scanned"] % 100 == 0:
                log.info(
                    f"File scanner: {stats['scanned']} scanned, "
                    f"{stats['new']+stats['changed']} sent, "
                    f"{stats['atoms_created']} atoms"
                )

    _save_state(new_state)
    stats["duration_s"] = round(time.time() - start, 1)
    stats["state_size"] = len(new_state)
    log.info(
        f"File scanner done: {stats['scanned']} scanned, "
        f"{stats['new']} new, {stats['changed']} changed, "
        f"{stats['skipped_unchanged']} unchanged, "
        f"{stats['atoms_created']} atoms in {stats['duration_s']}s"
    )
    return stats


async def trigger_scan() -> Dict[str, Any]:
    """Public entry point — called by scheduler and MCP tool."""
    return await run_file_scan()
