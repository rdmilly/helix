"""Helix Node Registry

Tracks registered nodes (workstations, servers, desktops) that have
a Helix Node Agent running. Nodes register on startup and optionally
heartbeat periodically. helix_action routes file ops to nodes by name.

Endpoints:
  POST   /api/v1/nodes/register    - register or update a node
  POST   /api/v1/nodes/heartbeat   - liveness ping from agent
  GET    /api/v1/nodes             - list all nodes
  GET    /api/v1/nodes/{node_id}   - get node details
  DELETE /api/v1/nodes/{node_id}   - remove a node
"""
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException
from services import pg_sync
from services.database import get_db_path
from pydantic import BaseModel

log = logging.getLogger("helix.nodes")
router = APIRouter(prefix="/api/v1/nodes", tags=["Nodes"])


class NodeRegistration(BaseModel):
    node_id: str
    node_name: str
    node_type: str = "workstation"   # workstation | vps | remote | desktop
    platform: str = "unknown"         # Windows | Darwin | Linux
    hostname: str = ""
    agent_url: Optional[str] = None   # HTTP URL if agent runs as server (future)
    tools: list = []
    registered_at: Optional[str] = None


class NodeHeartbeat(BaseModel):
    node_id: str
    node_name: str
    status: str = "online"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _conn():
    return pg_sync.sqlite_conn(str(get_db_path()))


def _ensure_table():
    try:
        conn = _conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                node_id     TEXT PRIMARY KEY,
                node_name   TEXT NOT NULL,
                node_type   TEXT DEFAULT 'workstation',
                platform    TEXT DEFAULT 'unknown',
                hostname    TEXT DEFAULT '',
                agent_url   TEXT,
                tools_json  TEXT DEFAULT '[]',
                status      TEXT DEFAULT 'online',
                registered_at TEXT,
                last_seen   TEXT,
                meta_json   TEXT DEFAULT '{}'
            )
        """)
        conn.commit()
    except Exception as e:
        log.warning(f"nodes table ensure: {e}")


try:
    _ensure_table()
except Exception:
    pass


@router.post("/register")
async def register_node(reg: NodeRegistration):
    """Register a node or update it if already registered."""
    _ensure_table()
    conn = _conn()
    now = _now()
    try:
        existing = conn.execute(
            "SELECT node_id FROM nodes WHERE node_id = ?", (reg.node_id,)
        ).fetchone()

        if existing:
            conn.execute("""
                UPDATE nodes SET
                    node_name=?, node_type=?, platform=?, hostname=?,
                    agent_url=?, tools_json=?, status='online', last_seen=?
                WHERE node_id=?
            """, (reg.node_name, reg.node_type, reg.platform, reg.hostname,
                  reg.agent_url, json.dumps(reg.tools), now, reg.node_id))
            action = "updated"
        else:
            conn.execute("""
                INSERT INTO nodes
                    (node_id, node_name, node_type, platform, hostname,
                     agent_url, tools_json, status, registered_at, last_seen)
                VALUES (?,?,?,?,?,?,?,'online',?,?)
            """, (reg.node_id, reg.node_name, reg.node_type, reg.platform,
                  reg.hostname, reg.agent_url, json.dumps(reg.tools),
                  reg.registered_at or now, now))
            action = "registered"

        conn.commit()
        log.info(f"Node {action}: {reg.node_name} ({reg.node_id}) [{reg.platform}]")
        return {"status": action, "node_id": reg.node_id, "node_name": reg.node_name, "timestamp": now}
    except Exception as e:
        log.error(f"Node registration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/heartbeat")
async def node_heartbeat(hb: NodeHeartbeat):
    """Update node last_seen. Called periodically by agent."""
    try:
        conn = _conn()
        conn.execute(
            "UPDATE nodes SET last_seen=?, status=? WHERE node_id=?",
            (_now(), hb.status, hb.node_id)
        )
        conn.commit()
        return {"status": "ok", "node_id": hb.node_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("")
async def list_nodes():
    """List all registered nodes."""
    _ensure_table()
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT node_id, node_name, node_type, platform, hostname, "
            "agent_url, status, registered_at, last_seen "
            "FROM nodes ORDER BY last_seen DESC"
        ).fetchall()
        return {
            "nodes": [
                {
                    "node_id": r[0], "node_name": r[1], "node_type": r[2],
                    "platform": r[3], "hostname": r[4], "agent_url": r[5],
                    "status": r[6], "registered_at": r[7], "last_seen": r[8]
                } for r in rows
            ],
            "count": len(rows)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{node_id}")
async def get_node(node_id: str):
    """Get a specific node by ID."""
    _ensure_table()
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT node_id, node_name, node_type, platform, hostname, "
            "agent_url, tools_json, status, registered_at, last_seen "
            "FROM nodes WHERE node_id=?", (node_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")
        return {
            "node_id": row[0], "node_name": row[1], "node_type": row[2],
            "platform": row[3], "hostname": row[4], "agent_url": row[5],
            "tools": json.loads(row[6] or "[]"), "status": row[7],
            "registered_at": row[8], "last_seen": row[9]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{node_id}")
async def remove_node(node_id: str):
    """Remove a node from the registry."""
    try:
        conn = _conn()
        conn.execute("DELETE FROM nodes WHERE node_id=?", (node_id,))
        conn.commit()
        return {"status": "removed", "node_id": node_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
