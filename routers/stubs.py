"""Stub Routers for Phase 7+

Phase 3 (Synapse API) is now live — see routers/synapse.py.
Phase 4 (Compression Engine) is now live — see routers/compression.py.
Phase 5 (Editor & Expressions) is now live — see routers/editor.py.
Phase 6 (Nervous System / Cockpit) is now live — see routers/cockpit.py.

Remaining stubs return 501 Not Implemented with helpful messages.
"""
from fastapi import APIRouter
from fastapi.responses import JSONResponse


# === FLAGELLA ROUTER (Phase 7) ===

flagella_router = APIRouter(prefix="/api/v1/flagella")


@flagella_router.get("/tools")
async def list_mcp_tools():
    """List available MCP tools"""
    return JSONResponse(
        status_code=501,
        content={
            "error": "Not Implemented",
            "message": "Flagella (MCP server) will be available in Phase 7",
            "phase": "Phase 7: Flagella MCP Server"
        }
    )


@flagella_router.post("/execute")
async def execute_mcp_tool():
    """Execute an MCP tool"""
    return JSONResponse(
        status_code=501,
        content={
            "error": "Not Implemented",
            "message": "MCP tool execution will be available in Phase 7",
            "phase": "Phase 7: Flagella MCP Server"
        }
    )
