"""
tenants.py — Layer 0 tenant provisioning
"""
import logging
import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from services import pg_sync
from services.tenant_auth import generate_api_key
from services.email import send_invite as _email_invite

logger = logging.getLogger(__name__)
tenants_router = APIRouter(prefix="/api/v1/admin")

MAIL_SVC = "http://helix-cortex:9050"  # internal; will use pg_sync pattern instead


class TenantCreate(BaseModel):
    slug: str
    name: str
    plan: str = "free"
    key_name: Optional[str] = "default"
    email: Optional[str] = None   # if set, send invite email


def _send_invite(email: str, name: str, slug: str, setup_url: str):
    """Fire invite via mail MCP (notifications@millyweb.com). Non-fatal."""
    try:
        # Call the internal mail endpoint directly via pg_sync http client
        # Mail svc is on provisioner — we POST to our own /api/v1/admin/send-invite
        # which will be called by the admin after creation, OR
        # we log it and let the calling session handle it via MCP.
        # For now: log invite details so the session can trigger mail__mail_send.
        logger.info(f"INVITE_PENDING email={email} slug={slug} setup_url={setup_url}")
    except Exception as e:
        logger.warning(f"invite log failed: {e}")


@tenants_router.post("/tenants")
def create_tenant(body: TenantCreate):
    slug = body.slug.lower().strip()
    if not slug or not slug.replace("-","").replace("_","").isalnum():
        raise HTTPException(400, "slug must be alphanumeric")
    if slug == "system":
        raise HTTPException(400, "reserved")

    raw_key, key_hash = generate_api_key()

    try:
        with pg_sync.get_pg_conn(admin=True) as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM tenants WHERE slug = %s", (slug,))
            if cur.fetchone():
                raise HTTPException(409, f"slug '{slug}' exists")
            cur.execute(
                "INSERT INTO tenants (slug, name, plan) VALUES (%s, %s, %s)",
                (slug, body.name, body.plan)
            )
            cur.execute("SELECT id, slug, name, plan, status FROM tenants WHERE slug = %s", (slug,))
            row = cur.fetchone()
            tenant_id = row[0]
            cur.execute(
                "INSERT INTO api_keys (tenant_id, key_hash, name) VALUES (%s, %s, %s)",
                (tenant_id, key_hash, body.key_name)
            )
            # Store email in meta if provided
            if body.email:
                cur.execute(
                    "UPDATE tenants SET meta = meta || %s::jsonb WHERE id = %s",
                    (f'{{"email": "{body.email}"}}', tenant_id)
                )
            conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"create_tenant: {e}")
        raise HTTPException(500, str(e))

    base = f"https://{slug}.helix.millyweb.com"
    setup_url = f"{base}/dashboard/home?key={raw_key}"

    if body.email:
        _email_invite(body.email, body.name, slug, setup_url)

    logger.info(f"tenant created: {slug} ({tenant_id}) email={body.email}")
    return {
        "id": tenant_id,
        "slug": row[1],
        "name": row[2],
        "plan": row[3],
        "status": row[4],
        "api_key": raw_key,
        "helix_url": base,
        "dashboard_url": f"{base}/dashboard",
        "setup_url": setup_url,
        "invite_email": body.email,
    }


@tenants_router.get("/tenants")
def list_tenants():
    try:
        with pg_sync.get_pg_conn(admin=True) as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, slug, name, plan, status, created_at, meta FROM tenants ORDER BY created_at")
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(500, str(e))
    return [{"id": r[0], "slug": r[1], "name": r[2], "plan": r[3],
             "status": r[4], "created_at": str(r[5]),
             "email": (r[6] or {}).get("email")} for r in rows]


@tenants_router.post("/tenants/{tenant_id}/keys")
def create_api_key(tenant_id: str, key_name: str = "default"):
    raw_key, key_hash = generate_api_key()
    try:
        with pg_sync.get_pg_conn(admin=True) as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM tenants WHERE id = %s AND status = 'active'", (tenant_id,))
            if not cur.fetchone():
                raise HTTPException(404, "tenant not found")
            cur.execute(
                "INSERT INTO api_keys (tenant_id, key_hash, name) VALUES (%s, %s, %s)",
                (tenant_id, key_hash, key_name)
            )
            conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))
    return {"api_key": raw_key, "tenant_id": tenant_id}
