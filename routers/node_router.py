"""Node Router — routes helix_action calls to registered nodes

Called by action.py when a `node` parameter is specified.
Supports:
  - vps1 / vps2: SSH write via gateway (current behavior, no change)
  - workstation nodes: HTTP POST to agent running on that machine
  - staging: VPS2 -> staging_push -> Windows (fallback for Windows)
"""
import logging
from typing import Optional

import httpx

log = logging.getLogger("helix.node_router")

# Well-known VPS nodes that use SSH directly
SSH_NODES = {
    "vps1": "72.60.31.69",
    "vps2": "72.60.225.81",
}


async def route_file_write(node: str, path: str, content: str, session_id: str = "helix_action") -> dict:
    """
    Route a file write to the appropriate node.
    - vps1/vps2: write directly (local filesystem from container, or SSH)
    - agent nodes: POST to node agent HTTP endpoint
    - None/missing: default to local VPS1 (existing behavior)
    """
    node = (node or "").lower().strip()

    # Default / vps1 = local write (current behavior, no change)
    if not node or node == "vps1":
        return None  # signals: use local workbench write

    # Other well-known SSH nodes
    if node in SSH_NODES:
        return await _ssh_write(node, SSH_NODES[node], path, content)

    # Look up agent URL from node registry
    agent_url = await _get_agent_url(node)
    if agent_url:
        return await _agent_write(agent_url, path, content)

    return {"status": "error", "error": f"Unknown node: '{node}'. Register it at /api/v1/nodes/register"}


async def route_file_read(node: str, path: str) -> dict:
    node = (node or "").lower().strip()
    if not node or node == "vps1":
        return None
    agent_url = await _get_agent_url(node)
    if agent_url:
        return await _agent_read(agent_url, path)
    return {"status": "error", "error": f"Unknown node: '{node}'"}


async def route_command(node: str, command: str, timeout: int = 30) -> dict:
    node = (node or "").lower().strip()
    if not node or node == "vps1":
        return None
    agent_url = await _get_agent_url(node)
    if agent_url:
        return await _agent_command(agent_url, command, timeout)
    return {"status": "error", "error": f"Unknown node: '{node}'"}


async def list_nodes() -> list:
    """Return all registered nodes from the registry."""
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get("http://localhost:9050/api/v1/nodes")
            if r.status_code == 200:
                return r.json().get("nodes", [])
    except Exception as e:
        log.debug(f"Node list failed: {e}")
    return []


# ── Internal helpers ──────────────────────────────────────────────────────────────────

async def _get_agent_url(node_name: str) -> Optional[str]:
    """Look up agent_url for a node from the registry."""
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get("http://localhost:9050/api/v1/nodes")
            if r.status_code == 200:
                for n in r.json().get("nodes", []):
                    if n["node_name"].lower() == node_name:
                        return n.get("agent_url")
    except Exception as e:
        log.debug(f"Agent URL lookup failed: {e}")
    return None


async def _ssh_write(node_name: str, host: str, path: str, content: str) -> dict:
    """Write to a VPS node via SSH (gateway)."""
    try:
        from services.workbench import run_shell
        # Write via SSH using the gateway pattern
        escaped = content.replace("'", "'\"'\"'")
        result = run_shell(f"ssh -o StrictHostKeyChecking=no root@{host} 'cat > {path}'")
        return {"status": "written", "path": path, "node": node_name, "method": "ssh"}
    except Exception as e:
        return {"status": "error", "error": str(e), "node": node_name}


async def _agent_write(agent_url: str, path: str, content: str) -> dict:
    """POST file write to a node agent."""
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post(f"{agent_url}/api/file_write", json={"path": path, "content": content})
            return r.json()
    except Exception as e:
        return {"status": "error", "error": f"Agent unreachable: {e}"}


async def _agent_read(agent_url: str, path: str) -> dict:
    """GET file read from a node agent."""
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f"{agent_url}/api/file_read", params={"path": path})
            return r.json()
    except Exception as e:
        return {"status": "error", "error": f"Agent unreachable: {e}"}


async def _agent_command(agent_url: str, command: str, timeout: int) -> dict:
    """POST command to a node agent."""
    try:
        async with httpx.AsyncClient(timeout=timeout + 5) as c:
            r = await c.post(f"{agent_url}/api/command", json={"command": command, "timeout": timeout})
            return r.json()
    except Exception as e:
        return {"status": "error", "error": f"Agent unreachable: {e}"}
