"""The Forge — Workspace Router.

File CRUD operations backed by:
- Filesystem (primary, for active work)
- MinIO (versioned archive)
- SQLite FTS5 (full-text search)

Every write triggers pattern scanning via the scanner service.
"""

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from config import WORKSPACE_ROOT, SCANNABLE_EXTENSIONS
from services.database import get_db
from services.minio_client import put_file, get_file as minio_get, list_files as minio_list
import httpx
from config import HELIX_URL  # Cortex scanner webhook

logger = logging.getLogger("forge.workspace")
router = APIRouter(prefix="/api/workspace", tags=["Workspace"])

# -----------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------

class FileWrite(BaseModel):
    path: str = Field(..., description="Relative path within workspace (e.g. 'my-project/server.py')")
    content: str
    project: Optional[str] = None
    source: str = "api"  # api, observer, cli
    deploy_path: Optional[str] = Field(None, description="VPS target path. If set, deploys after workspace write.")

class FileMove(BaseModel):
    source: str
    destination: str

# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _detect_language(path: str) -> str | None:
    ext_map = {
        '.py': 'python', '.js': 'javascript', '.ts': 'typescript',
        '.jsx': 'react', '.tsx': 'react-ts', '.html': 'html',
        '.css': 'css', '.sh': 'bash', '.yaml': 'yaml', '.yml': 'yaml',
        '.json': 'json', '.md': 'markdown', '.sql': 'sql',
        '.toml': 'toml', '.cfg': 'config', '.conf': 'config',
    }
    lower = path.lower()
    if lower.endswith('dockerfile') or 'dockerfile' in lower:
        return 'dockerfile'
    if 'docker-compose' in lower:
        return 'docker-compose'
    for ext, lang in ext_map.items():
        if lower.endswith(ext):
            return lang
    return None

def _is_scannable(path: str) -> bool:
    lower = path.lower()
    if 'dockerfile' in lower or 'docker-compose' in lower:
        return True
    return any(lower.endswith(ext) for ext in SCANNABLE_EXTENSIONS)

def _infer_project(path: str) -> str | None:
    """Infer project name from path."""
    parts = Path(path).parts
    if len(parts) >= 1:
        return parts[0]
    return None

def _estimate_tokens(text: str) -> int:
    return len(text) // 4

# -----------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------

# Path mapping: VPS paths -> container mount points
VPS_PATH_MAP = {
    "/opt/projects": Path("/projects"),
    "/opt/stacks": Path("/stacks"),
}

def _deploy_to_vps(target_path: str, content: str) -> dict:
    """Deploy a file to its target path on the VPS via mounted volumes."""
    # Map VPS path to container path
    container_path = None
    for vps_prefix, mount_point in VPS_PATH_MAP.items():
        if target_path.startswith(vps_prefix):
            relative = target_path[len(vps_prefix):]
            container_path = mount_point / relative.lstrip('/')
            break
    
    if not container_path:
        return {"error": f"Path not in mounted volumes. Available: /opt/projects, /opt/stacks", "target": target_path}
    
    try:
        container_path.parent.mkdir(parents=True, exist_ok=True)
        container_path.write_text(content, encoding='utf-8')
        logger.info(f"Deployed to VPS: {target_path} ({len(content)} bytes)")
        return {"target": target_path, "size_bytes": len(content), "status": "deployed"}
    except Exception as e:
        logger.error(f"Deploy failed for {target_path}: {e}")
        return {"error": str(e), "target": target_path}


@router.post("/write")
async def write_file(req: FileWrite):
    """Write a file to the workspace.
    
    This is the core operation. Every write:
    1. Saves to filesystem
    2. Stores in MinIO (versioned)
    3. Indexes for full-text search
    4. Scans for atoms (if code file)
    """
    # Sanitize path
    clean_path = req.path.lstrip('/').replace('..', '')
    if not clean_path:
        raise HTTPException(400, "Invalid path")
    
    full_path = WORKSPACE_ROOT / clean_path
    project = req.project or _infer_project(clean_path)
    language = _detect_language(clean_path)
    content_hash = hashlib.sha256(req.content.encode()).hexdigest()[:16]
    
    # 1. Write to filesystem
    full_path.parent.mkdir(parents=True, exist_ok=True)
    full_path.write_text(req.content, encoding='utf-8')
    
    # 2. Store in MinIO (versioned)
    version_id = put_file(clean_path, req.content)
    
    # 3. Index in database + FTS
    db = get_db()
    try:
        existing = db.execute("SELECT id FROM files WHERE path = ?", (clean_path,)).fetchone()
        
        if existing:
            file_id = existing['id']
            # Get current version count
            ver = db.execute(
                "SELECT MAX(version_num) as v FROM file_versions WHERE file_id = ?",
                (file_id,)
            ).fetchone()
            version_num = (ver['v'] or 0) + 1
            
            db.execute(
                """UPDATE files SET size_bytes=?, line_count=?, token_estimate=?,
                   content_hash=?, minio_version=?, language=?, project=?,
                   updated_at=datetime('now'), deleted=0
                   WHERE id=?""",
                (len(req.content), req.content.count('\n') + 1,
                 _estimate_tokens(req.content), content_hash, version_id,
                 language, project, file_id)
            )
            # Update FTS
            db.execute("DELETE FROM files_fts WHERE path = ?", (clean_path,))
        else:
            cursor = db.execute(
                """INSERT INTO files (path, project, filename, extension, language,
                   size_bytes, line_count, token_estimate, content_hash, minio_version)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (clean_path, project, Path(clean_path).name,
                 Path(clean_path).suffix, language,
                 len(req.content), req.content.count('\n') + 1,
                 _estimate_tokens(req.content), content_hash, version_id)
            )
            file_id = cursor.lastrowid
            version_num = 1
        
        # Version history
        db.execute(
            """INSERT INTO file_versions (file_id, version_num, content_hash, size_bytes, minio_version)
               VALUES (?, ?, ?, ?, ?)""",
            (file_id, version_num, content_hash, len(req.content), version_id)
        )
        
        # FTS index
        db.execute(
            "INSERT INTO files_fts (path, project, filename, content) VALUES (?, ?, ?, ?)",
            (clean_path, project or '', Path(clean_path).name, req.content)
        )
        
        db.commit()
    finally:
        db.close()
    
    # 4. Scan for atoms via Helix Cortex webhook (if scannable)
    scan_result = None
    if _is_scannable(clean_path) and language:
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(
                    f"{HELIX_URL}/api/v1/scan/webhook",
                    json={
                        "path": str(req.deploy_path or full_path),
                        "content": req.content,
                        "project": project or "",
                        "event": "write",
                        "source": "forge",
                    }
                )
                if resp.status_code == 200:
                    scan_result = resp.json()
                    logger.info(f"Cortex scanned {clean_path}: {scan_result.get('atoms_created', 0)} created, {scan_result.get('atoms_updated', 0)} updated")
                else:
                    logger.warning(f"Cortex scan failed for {clean_path}: {resp.status_code}")
                    scan_result = {"status": "error", "detail": f"Cortex returned {resp.status_code}"}
        except Exception as e:
            logger.warning(f"Cortex scan unreachable for {clean_path}: {e}")
            scan_result = {"status": "unreachable", "detail": str(e)}
    
    # 5. Deploy to VPS target path (if specified)
    deployed = None
    if req.deploy_path:
        deployed = _deploy_to_vps(req.deploy_path, req.content)
    
    return {
        "status": "written",
        "path": clean_path,
        "project": project,
        "language": language,
        "size_bytes": len(req.content),
        "version": version_num if 'version_num' in dir() else 1,
        "minio_version": version_id,
        "scan": scan_result,
        "deployed": deployed
    }


@router.get("/read")
async def read_file(path: str, version: Optional[str] = None):
    """Read a file from the workspace."""
    clean_path = path.lstrip('/').replace('..', '')
    full_path = WORKSPACE_ROOT / clean_path
    
    # Try filesystem first (fastest)
    if not version and full_path.exists():
        content = full_path.read_text(encoding='utf-8')
        return {"path": clean_path, "content": content, "source": "filesystem"}
    
    # Fall back to MinIO (for specific versions or deleted files)
    content = minio_get(clean_path, version_id=version)
    if content:
        return {"path": clean_path, "content": content, "source": "minio", "version": version}
    
    raise HTTPException(404, f"File not found: {clean_path}")


@router.get("/list")
async def list_directory(path: str = "", include_metadata: bool = False):
    """List files in the workspace."""
    clean_path = path.lstrip('/').replace('..', '')
    full_path = WORKSPACE_ROOT / clean_path if clean_path else WORKSPACE_ROOT
    
    if not full_path.exists():
        raise HTTPException(404, f"Directory not found: {clean_path}")
    
    entries = []
    for item in sorted(full_path.iterdir()):
        rel_path = str(item.relative_to(WORKSPACE_ROOT))
        entry = {
            "name": item.name,
            "path": rel_path,
            "is_directory": item.is_dir()
        }
        if not item.is_dir() and include_metadata:
            stat = item.stat()
            entry["size_bytes"] = stat.st_size
            entry["language"] = _detect_language(rel_path)
        entries.append(entry)
    
    return {"path": clean_path or "/", "entries": entries, "count": len(entries)}


@router.get("/search")
async def search_files(q: str, limit: int = 20):
    """Full-text search across workspace files."""
    db = get_db()
    try:
        rows = db.execute(
            """SELECT path, project, filename,
                      snippet(files_fts, 3, '<mark>', '</mark>', '...', 40) as snippet,
                      rank
               FROM files_fts
               WHERE files_fts MATCH ?
               ORDER BY rank
               LIMIT ?""",
            (q, limit)
        ).fetchall()
        return {
            "query": q,
            "count": len(rows),
            "results": [
                {"path": r['path'], "project": r['project'],
                 "filename": r['filename'], "snippet": r['snippet']}
                for r in rows
            ]
        }
    finally:
        db.close()


@router.delete("/delete")
async def delete_file_endpoint(path: str):
    """Soft-delete a file (marks deleted, preserves in MinIO)."""
    clean_path = path.lstrip('/').replace('..', '')
    full_path = WORKSPACE_ROOT / clean_path
    
    db = get_db()
    try:
        db.execute("UPDATE files SET deleted=1, updated_at=datetime('now') WHERE path=?", (clean_path,))
        db.execute("DELETE FROM files_fts WHERE path=?", (clean_path,))
        db.commit()
    finally:
        db.close()
    
    # Remove from filesystem (MinIO versioning preserves)
    if full_path.exists():
        full_path.unlink()
    
    return {"status": "deleted", "path": clean_path}


@router.post("/move")
async def move_file(req: FileMove):
    """Move or rename a file."""
    src = req.source.lstrip('/').replace('..', '')
    dst = req.destination.lstrip('/').replace('..', '')
    src_full = WORKSPACE_ROOT / src
    dst_full = WORKSPACE_ROOT / dst
    
    if not src_full.exists():
        raise HTTPException(404, f"Source not found: {src}")
    
    dst_full.parent.mkdir(parents=True, exist_ok=True)
    src_full.rename(dst_full)
    
    db = get_db()
    try:
        db.execute("UPDATE files SET path=?, filename=?, project=?, updated_at=datetime('now') WHERE path=?",
                   (dst, Path(dst).name, _infer_project(dst), src))
        db.execute("DELETE FROM files_fts WHERE path=?", (src,))
        # Re-index at new path
        content = dst_full.read_text(encoding='utf-8')
        db.execute(
            "INSERT INTO files_fts (path, project, filename, content) VALUES (?, ?, ?, ?)",
            (dst, _infer_project(dst) or '', Path(dst).name, content)
        )
        db.commit()
    finally:
        db.close()
    
    return {"status": "moved", "from": src, "to": dst}


@router.get("/stats")
async def workspace_stats():
    """Get workspace statistics."""
    db = get_db()
    try:
        total = db.execute("SELECT COUNT(*) as c FROM files WHERE deleted=0").fetchone()['c']
        by_lang = db.execute(
            "SELECT language, COUNT(*) as c FROM files WHERE deleted=0 AND language IS NOT NULL GROUP BY language ORDER BY c DESC"
        ).fetchall()
        by_project = db.execute(
            "SELECT project, COUNT(*) as c FROM files WHERE deleted=0 AND project IS NOT NULL GROUP BY project ORDER BY c DESC LIMIT 20"
        ).fetchall()
        total_size = db.execute(
            "SELECT COALESCE(SUM(size_bytes), 0) as s FROM files WHERE deleted=0"
        ).fetchone()['s']
        versions = db.execute("SELECT COUNT(*) as c FROM file_versions").fetchone()['c']
        
        return {
            "total_files": total,
            "total_size_bytes": total_size,
            "total_versions": versions,
            "by_language": {r['language']: r['c'] for r in by_lang},
            "by_project": {r['project']: r['c'] for r in by_project}
        }
    finally:
        db.close()
