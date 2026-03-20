"""MCP StreamableHTTP mount for Helix Cortex.

Provides:
- setup_mcp(app): call during lifespan to initialize session manager
- teardown_mcp(): call during shutdown
- mcp_asgi_app: raw ASGI app to mount at /mcp
"""
import logging
from contextlib import asynccontextmanager
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp_tools import mcp as helix_mcp

logger = logging.getLogger(__name__)

# Module-level state
_session_mgr: StreamableHTTPSessionManager | None = None
_ctx_manager = None


async def setup_mcp():
    """Initialize and start MCP session manager. Call during app lifespan."""
    global _session_mgr, _ctx_manager

    helix_mcp.settings.stateless_http = True
    _session_mgr = StreamableHTTPSessionManager(
        app=helix_mcp._mcp_server,
        event_store=helix_mcp._event_store,
        json_response=helix_mcp.settings.json_response,
        stateless=True,
    )
    _ctx_manager = _session_mgr.run()
    await _ctx_manager.__aenter__()
    logger.info("MCP StreamableHTTP session manager started")


async def teardown_mcp():
    """Stop MCP session manager. Call during app shutdown."""
    global _ctx_manager
    if _ctx_manager:
        await _ctx_manager.__aexit__(None, None, None)
        logger.info("MCP StreamableHTTP session manager stopped")


async def mcp_asgi_app(scope, receive, send):
    """Raw ASGI app that delegates to the session manager."""
    if _session_mgr is None:
        # Not ready yet
        from starlette.responses import PlainTextResponse
        response = PlainTextResponse("MCP not initialized", status_code=503)
        await response(scope, receive, send)
        return
    await _session_mgr.handle_request(scope, receive, send)
