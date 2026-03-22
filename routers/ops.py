"""Operations Router — Retention, diagnostics, housekeeping.

Endpoints:
  POST /api/v1/ops/retention       - Run retention policies
  GET  /api/v1/ops/diagnostics     - Full system health + data inventory
  POST /api/v1/ops/vacuum          - SQLite VACUUM for space reclamation
  GET  /api/v1/ops/db-size         - Database file sizes
"""
import os
import sqlite3
from services import pg_sync
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict
from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel
from services.database import get_db_path
from services.retention import run_retention

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/ops")

DATA_DIR = Path("/app/data")


class RetentionRequest(BaseModel):
    overrides: Optional[Dict[str, int]] = None

@router.post("/retention")
async def trigger_retention(req: RetentionRequest = RetentionRequest()):
    results = run_retention(overrides=req.overrides)
    return {"status": "completed", "results": results}


@router.get("/diagnostics")
async def diagnostics():
    # Use sqlite3 directly — sqlite_master is SQLite-only syntax,
    # pg_sync routes through psycopg2 and breaks on it.
    conn = sqlite3.connect(str(get_db_path()), timeout=10)
    conn.row_factory = sqlite3.Row
    tables = {}
    try:
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
            name = row['name']
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
                tables[name] = count
            except:
                tables[name] = "error"
    finally:
        conn.close()

    # DB file sizes
    db_sizes = {}
    for f in DATA_DIR.glob("*.db*"):
        db_sizes[f.name] = round(f.stat().st_size / 1024 / 1024, 2)

    # Backup count
    backup_dir = DATA_DIR / "backups"
    backup_count = len(list(backup_dir.iterdir())) if backup_dir.exists() else 0

    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tables": tables,
        "db_sizes_mb": db_sizes,
        "backup_count": backup_count,
        "data_dir": str(DATA_DIR),
    }


@router.post("/vacuum")
async def vacuum_db(background_tasks: BackgroundTasks):
    def _vacuum():
        # Use sqlite3 directly — VACUUM is SQLite-only
        conn = sqlite3.connect(str(get_db_path()), timeout=60)
        before = os.path.getsize(str(get_db_path()))
        conn.execute("VACUUM")
        conn.close()
        after = os.path.getsize(str(get_db_path()))
        saved = before - after
        logger.info(f"VACUUM saved {saved} bytes ({round(saved/1024/1024, 2)} MB)")
    background_tasks.add_task(_vacuum)
    return {"status": "vacuum_started"}


@router.get("/db-size")
async def db_sizes():
    sizes = {}
    for f in DATA_DIR.glob("*.db*"):
        sizes[f.name] = {"bytes": f.stat().st_size, "mb": round(f.stat().st_size / 1024 / 1024, 2)}
    total = sum(s["bytes"] for s in sizes.values())
    return {"files": sizes, "total_mb": round(total / 1024 / 1024, 2)}
