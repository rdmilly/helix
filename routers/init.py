"""
init.py — Layer 1: helix_init() installer endpoint

POST /api/v1/init
  - Resolves tenant from API key or subdomain (via TenantMiddleware)
  - Issues a fresh node API key for the machine being set up
  - Returns Desktop config.json snippet + agent download URL

The client: copies the config snippet into claude_desktop_config.json,
deploys agent.py to C:/tools/helix-node/ (or ~/helix-node/),
restarts Claude Desktop. Done.
"""
import json
import logging
import socket
from typing import Optional
from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from services import pg_sync
from services.tenant_auth import generate_api_key

logger = logging.getLogger(__name__)
init_router = APIRouter(prefix="/api/v1")

HELIX_BASE = "https://helix.millyweb.com"
AGENT_DOWNLOAD = "https://helix.millyweb.com/api/v1/init/agent"


class InitRequest(BaseModel):
    node_name: Optional[str] = None   # e.g. "ashley-laptop"
    platform: Optional[str] = "windows"  # windows | mac | linux
    agent_path: Optional[str] = None  # override default install path


@init_router.post("/init")
def helix_init(body: InitRequest, request: Request):
    """Generate install payload for a new machine.
    Requires X-Helix-API-Key or subdomain routing for tenant resolution.
    """
    tenant_id = getattr(request.state, "tenant_id", "system")
    if tenant_id == "system":
        raise HTTPException(401, "API key required for init")

    # Resolve tenant slug for subdomain URL
    try:
        with pg_sync.get_pg_conn(admin=True) as conn:
            cur = conn.cursor()
            cur.execute("SELECT slug, name FROM tenants WHERE id = %s", (tenant_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "tenant not found")
            slug, tenant_name = row[0], row[1]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

    # Issue a fresh API key for this node
    raw_key, key_hash = generate_api_key()
    node_name = body.node_name or f"{slug}-machine"
    key_label = f"node:{node_name}"

    try:
        with pg_sync.get_pg_conn(admin=True) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO api_keys (tenant_id, key_hash, name) VALUES (%s, %s, %s)",
                (tenant_id, key_hash, key_label)
            )
            conn.commit()
    except Exception as e:
        raise HTTPException(500, f"key issue failed: {e}")

    # Build platform-specific defaults
    if body.platform == "windows":
        default_path = "C:/tools/helix-node/agent.py"
        python_cmd = "python"
    elif body.platform == "mac":
        default_path = "/usr/local/lib/helix-node/agent.py"
        python_cmd = "python3"
    else:  # linux
        default_path = "/opt/helix-node/agent.py"
        python_cmd = "python3"

    agent_path = body.agent_path or default_path
    helix_url = f"https://{slug}.helix.millyweb.com"

    # Desktop config snippet
    desktop_config = {
        "mcpServers": {
            "helix": {
                "command": python_cmd,
                "args": [agent_path],
                "env": {
                    "HELIX_URL": helix_url,
                    "HELIX_API_KEY": raw_key,
                    "NODE_NAME": node_name,
                }
            }
        }
    }

    logger.info(f"init: tenant={slug} node={node_name}")
    return {
        "tenant": slug,
        "tenant_name": tenant_name,
        "node_name": node_name,
        "helix_url": helix_url,
        "api_key": raw_key,
        "agent_download": AGENT_DOWNLOAD,
        "agent_path": agent_path,
        "desktop_config": desktop_config,
        "desktop_config_json": json.dumps(desktop_config, indent=2),
        "instructions": [
            f"1. Download agent: {AGENT_DOWNLOAD}",
            f"2. Save to: {agent_path}",
            f"3. Merge desktop_config_json into your claude_desktop_config.json",
            "4. Restart Claude Desktop",
            "5. Verify: ask Claude to run node_status()",
        ]
    }


@init_router.get("/init/agent")
def download_agent():
    """Serve the helix-node agent.py for download."""
    try:
        content = open("/opt/projects/helix-node/agent.py").read()
    except Exception as e:
        raise HTTPException(500, str(e))
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse(content, media_type="text/plain",
                             headers={"Content-Disposition": "attachment; filename=agent.py"})
