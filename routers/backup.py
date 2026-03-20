"""Backup & Restore Router for Helix Cortex.

Backups include:
- cortex.db (FTS conversations, structured archive, KB docs, knowledge graph, observer data)
- ChromaDB collections (vector embeddings)
- Runbook pages

Endpoints:
  GET  /api/v1/backup/list    - List available backups
  POST /api/v1/backup/create  - Create a backup
  POST /api/v1/backup/restore - Restore from a backup
  POST /api/v1/backup/prune   - Remove old backups
"""
import json
from services import pg_sync
import shutil
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/backup")

DATA_DIR = Path("/app/data")
BACKUP_DIR = DATA_DIR / "backups"
MAX_BACKUPS = 10


def _backup_path(timestamp: str = None) -> Path:
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    return BACKUP_DIR / timestamp


def _prune_old_backups(keep: int = MAX_BACKUPS):
    if not BACKUP_DIR.exists():
        return 0
    backups = sorted(BACKUP_DIR.iterdir(), key=lambda p: p.name)
    pruned = 0
    while len(backups) > keep:
        old = backups.pop(0)
        if old.is_dir():
            shutil.rmtree(old)
            pruned += 1
    return pruned


def _create_backup(tag: str = "") -> dict:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    backup_dir = _backup_path(ts)
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    files_backed_up = []
    errors = []
    
    # 1. Backup cortex.db (main SQLite database)
    cortex_db = DATA_DIR / "cortex.db"
    if cortex_db.exists():
        try:
            import sqlite3
            src = sqlite3.connect(str(cortex_db))
            dst = sqlite3.connect(str(backup_dir / "cortex.db"))
            src.backup(dst)
            src.close()
            dst.close()
            files_backed_up.append("cortex.db")
        except Exception as e:
            errors.append(f"cortex.db: {e}")
    
    # 2. Backup any other .db files
    for db_file in DATA_DIR.glob("*.db"):
        if db_file.name == "cortex.db":
            continue
        try:
            shutil.copy2(db_file, backup_dir / db_file.name)
            files_backed_up.append(db_file.name)
        except Exception as e:
            errors.append(f"{db_file.name}: {e}")
    
    # 3. Backup runbook data
    runbook_dir = DATA_DIR / "runbook"
    if runbook_dir.exists():
        try:
            shutil.copytree(runbook_dir, backup_dir / "runbook")
            files_backed_up.append("runbook/")
        except Exception as e:
            errors.append(f"runbook: {e}")
    
    # 4. Write manifest
    manifest = {
        "timestamp": ts,
        "tag": tag,
        "files": files_backed_up,
        "errors": errors,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (backup_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    
    # 5. Prune old backups
    pruned = _prune_old_backups()
    manifest["pruned"] = pruned
    
    return manifest


@router.get("/list")
async def list_backups():
    if not BACKUP_DIR.exists():
        return {"backups": [], "count": 0}
    backups = []
    for d in sorted(BACKUP_DIR.iterdir(), reverse=True):
        if not d.is_dir():
            continue
        manifest_path = d / "manifest.json"
        if manifest_path.exists():
            manifest = pg_sync.dejson(manifest_path.read_text())
        else:
            manifest = {"timestamp": d.name, "files": list(f.name for f in d.iterdir())}
        size = sum(f.stat().st_size for f in d.rglob('*') if f.is_file())
        backups.append({
            "name": d.name,
            "manifest": manifest,
            "size_bytes": size,
            "size_mb": round(size / 1024 / 1024, 2),
        })
    return {"backups": backups, "count": len(backups)}


class CreateBackupRequest(BaseModel):
    tag: str = ""

@router.post("/create")
async def create_backup(req: CreateBackupRequest = CreateBackupRequest(), background_tasks: BackgroundTasks = None):
    result = _create_backup(tag=req.tag)
    return {"status": "created", **result}


class RestoreRequest(BaseModel):
    backup_name: str
    components: Optional[List[str]] = None  # None = restore all

@router.post("/restore")
async def restore_backup(req: RestoreRequest):
    backup_dir = BACKUP_DIR / req.backup_name
    if not backup_dir.exists():
        return {"error": f"Backup {req.backup_name} not found"}
    
    restored = []
    errors = []
    components = req.components or ["cortex.db", "runbook"]
    
    if "cortex.db" in components:
        src = backup_dir / "cortex.db"
        if src.exists():
            try:
                import sqlite3
                s = sqlite3.connect(str(src))
                d = sqlite3.connect(str(DATA_DIR / "cortex.db"))
                s.backup(d)
                s.close()
                d.close()
                restored.append("cortex.db")
            except Exception as e:
                errors.append(f"cortex.db: {e}")
    
    if "runbook" in components:
        src = backup_dir / "runbook"
        if src.exists():
            try:
                dst = DATA_DIR / "runbook"
                if dst.exists():
                    shutil.rmtree(dst)
                shutil.copytree(src, dst)
                restored.append("runbook")
            except Exception as e:
                errors.append(f"runbook: {e}")
    
    return {"status": "restored", "backup": req.backup_name, "restored": restored, "errors": errors}


@router.post("/prune")
async def prune_backups(keep: int = MAX_BACKUPS):
    pruned = _prune_old_backups(keep)
    return {"status": "pruned", "removed": pruned, "kept": keep}


# ---------------------------------------------------------------------------
# Retention Policy
# ---------------------------------------------------------------------------
from services.retention import run_retention, DEFAULT_RETENTION_DAYS

@router.get("/retention/policy")
async def get_retention_policy():
    return {"policy": DEFAULT_RETENTION_DAYS}

@router.post("/retention/run")
async def execute_retention():
    results = run_retention()
    total_pruned = sum(r.get("pruned", 0) for r in results.values())
    return {"status": "complete", "total_pruned": total_pruned, "details": results}
