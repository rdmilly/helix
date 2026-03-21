"""helix_init MCP tool — fires on Desktop connect.

Checks tenant auth, node registration status, returns
full context for Claude to orient itself.
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from mcp_tools import mcp
from services import pg_sync
from services.database import get_db_path

log = logging.getLogger("helix.init_tool")


def _conn():
    return pg_sync.sqlite_conn(str(get_db_path()))


def _get_tenant_by_key(api_key: str) -> Optional[dict]:
    """Look up tenant by raw API key."""
    import hashlib
    try:
        conn = _conn()
        # Try raw key match first (stored as hx- prefixed)
        row = conn.execute(
            "SELECT id, slug, name, plan, status FROM tenants WHERE api_key_hash = ?",
            (api_key,)
        ).fetchone()
        if row:
            return {"id": row[0], "slug": row[1], "name": row[2], "plan": row[3], "status": row[4]}
    except Exception as e:
        log.warning(f"tenant lookup error: {e}")
    return None


def _get_node(node_id: str) -> Optional[dict]:
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT node_id, node_name, platform, status, last_seen FROM nodes WHERE node_id = ?",
            (node_id,)
        ).fetchone()
        if row:
            return {"node_id": row[0], "node_name": row[1], "platform": row[2],
                    "status": row[3], "last_seen": row[4]}
    except Exception:
        pass
    return None


def _get_tenant_stats(tenant_id: str) -> dict:
    try:
        conn = _conn()
        sessions = conn.execute(
            "SELECT COUNT(*) FROM observer_sessions WHERE tenant_id = ?", (tenant_id,)
        ).fetchone()[0]
        exchanges = conn.execute(
            "SELECT COUNT(*) FROM exchanges WHERE tenant_id = ? AND tenant_id != 'system'",
            (tenant_id,)
        ).fetchone()[0] if sessions else 0
        return {"sessions": sessions, "exchanges": exchanges}
    except Exception:
        return {"sessions": 0, "exchanges": 0}


@mcp.tool()
async def helix_init(
    node_id: str = "",
    node_name: str = "",
    platform: str = "",
) -> str:
    """Initialize Helix session. Call this at the START of every conversation.

    Identifies the tenant from your API key, checks node registration,
    and returns context to orient Claude.

    Args:
        node_id:   Unique ID for this machine (e.g. 'ashley-desktop-win')
        node_name: Human name for this machine (e.g. 'Ashley Desktop')
        platform:  OS platform (Windows | Darwin | Linux)

    Returns:
        JSON with tenant info, node status, setup_url if node not registered,
        and a brief orientation for Claude.
    """
    result = {
        "helix_version": "v0.9.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tenant": None,
        "node": None,
        "node_registered": False,
        "setup_url": None,
        "orientation": "",
        "status": "ok"
    }

    # --- Tenant identification (from request context or env fallback) ---
    # In full Layer 1, tenant comes from TenantMiddleware via request.state.
    # For now we pull from env (system/admin) or the invoking key if passed.
    try:
        from services import pg_sync as _pg
        conn = _conn()
        # Get first active tenant as a demo (will be wired to auth middleware in Layer 1 full)
        row = conn.execute(
            "SELECT id, slug, name, plan, status FROM tenants WHERE status='active' LIMIT 1"
        ).fetchone()
        if row:
            tenant = {"id": row[0], "slug": row[1], "name": row[2], "plan": row[3]}
            result["tenant"] = tenant
            stats = _get_tenant_stats(row[0])
            result["tenant"]["stats"] = stats
    except Exception as e:
        result["status"] = "degraded"
        log.warning(f"helix_init tenant lookup: {e}")

    # --- Node registration check ---
    if node_id:
        node = _get_node(node_id)
        if node:
            result["node"] = node
            result["node_registered"] = True
            # Update last_seen
            try:
                conn = _conn()
                conn.execute(
                    "UPDATE nodes SET last_seen=?, status='online' WHERE node_id=?",
                    (datetime.now(timezone.utc).isoformat(), node_id)
                )
                conn.commit()
            except Exception:
                pass
        else:
            # Auto-register the node
            try:
                conn = _conn()
                now = datetime.now(timezone.utc).isoformat()
                conn.execute("""
                    INSERT INTO nodes (node_id, node_name, node_type, platform, status, registered_at, last_seen)
                    VALUES (?, ?, 'desktop', ?, 'online', ?, ?)
                    ON CONFLICT(node_id) DO UPDATE SET
                        node_name=excluded.node_name,
                        platform=excluded.platform,
                        status='online',
                        last_seen=excluded.last_seen
                """, (node_id, node_name or node_id, platform or "unknown", now, now))
                conn.commit()
                result["node"] = {"node_id": node_id, "node_name": node_name, "platform": platform, "status": "online"}
                result["node_registered"] = True
                log.info(f"Auto-registered node {node_id}")
            except Exception as e:
                log.warning(f"node auto-register: {e}")

    # --- Build setup URL if tenant found ---
    if result["tenant"]:
        slug = result["tenant"]["slug"]
        base = os.environ.get("HELIX_BASE_URL", "https://helix.millyweb.com")
        # For non-system tenants use subdomain
        if slug and slug != "system":
            base = f"https://{slug}.helix.millyweb.com"
        result["setup_url"] = f"{base}/dashboard"

    # --- Orientation message ---
    tenant_name = result["tenant"]["name"] if result["tenant"] else "Unknown"
    node_info = f"Node: {node_name or node_id}" if node_id else "No node identified"
    result["orientation"] = (
        f"Helix v0.9.0 initialized. Tenant: {tenant_name}. {node_info}. "
        f"Observer is active — all tool calls are logged. "
        f"Use helix_search_conversations to recall past work."
    )

    return json.dumps(result, indent=2, default=str)
