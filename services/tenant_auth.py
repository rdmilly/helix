"""
tenant_auth.py — Layer 0 multi-tenancy middleware

Extracts tenant from:
  1. X-Helix-API-Key header  → lookup api_keys table
  2. X-Helix-Tenant-ID header (internal bypass)
  3. Host subdomain: ryan.helix.millyweb.com → slug 'ryan'

Fallback: 'system' (single-tenant compat).
"""
import hashlib
import secrets
import logging
from typing import Optional

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from services import pg_sync

logger = logging.getLogger(__name__)

SKIP_AUTH_PATHS = {
    "/health", "/ready", "/docs", "/redoc", "/openapi.json",
    "/api/v1/admin/tenants",
    "/api/v1/init/agent",
    "/dashboard",
    "/dashboard/home",
    "/register",
    "/login",
    "/api/v1/auth/register",
    "/api/v1/auth/login",
    "/widget",
}

INTERNAL_TENANT_HEADER = "X-Helix-Tenant-ID"
API_KEY_HEADER = "X-Helix-API-Key"


def hash_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


def generate_api_key() -> tuple:
    """Returns (raw_key, key_hash). Store hash only."""
    raw = "hx-" + secrets.token_urlsafe(32)
    return raw, hash_key(raw)


def extract_slug_from_host(host: str) -> Optional[str]:
    """ryan.helix.millyweb.com -> 'ryan'"""
    if not host:
        return None
    parts = host.split(".")
    if len(parts) == 4 and parts[1] == "helix":
        return parts[0]
    return None


def resolve_tenant(api_key_hash: Optional[str] = None,
                   slug: Optional[str] = None) -> Optional[str]:
    """Returns tenant_id or None."""
    try:
        with pg_sync.get_pg_conn() as conn:
            cur = conn.cursor()
            if api_key_hash:
                cur.execute(
                    """SELECT k.tenant_id FROM api_keys k
                       JOIN tenants t ON k.tenant_id = t.id
                       WHERE k.key_hash = %s AND k.status = 'active'
                         AND t.status = 'active'""",
                    (api_key_hash,)
                )
                row = cur.fetchone()
                if row:
                    cur2 = conn.cursor()
                    cur2.execute(
                        "UPDATE api_keys SET last_used_at = NOW() WHERE key_hash = %s",
                        (api_key_hash,)
                    )
                    conn.commit()
                    return row[0]
            if slug:
                cur.execute(
                    "SELECT id FROM tenants WHERE slug = %s AND status = 'active'",
                    (slug,)
                )
                row = cur.fetchone()
                return row[0] if row else None
    except Exception as e:
        logger.error(f"tenant resolve error: {e}")
    return None


class TenantMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if any(path.startswith(p) for p in SKIP_AUTH_PATHS) or path in SKIP_AUTH_PATHS:
            request.state.tenant_id = "system"
            return await call_next(request)

        tenant_id = None

        raw_key = request.headers.get(API_KEY_HEADER)
        if raw_key:
            tenant_id = resolve_tenant(api_key_hash=hash_key(raw_key))

        if not tenant_id:
            slug = request.headers.get(INTERNAL_TENANT_HEADER)
            if slug:
                tenant_id = resolve_tenant(slug=slug)

        if not tenant_id:
            host = request.headers.get("host", "")
            slug = extract_slug_from_host(host)
            if slug and slug != "helix":
                tenant_id = resolve_tenant(slug=slug)

        request.state.tenant_id = tenant_id or "system"
        return await call_next(request)


def set_tenant_context(tenant_id: str):
    """Set helix.tenant_id on DB connection before queries."""
    try:
        with pg_sync.get_pg_conn() as conn:
            cur = conn.cursor()
            cur.execute("SET LOCAL helix.tenant_id = %s", (tenant_id,))
    except Exception as e:
        logger.warning(f"set_tenant_context failed: {e}")
