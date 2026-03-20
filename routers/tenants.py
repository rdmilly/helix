"""
tenants.py — Layer 0 tenant provisioning
"""
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from services import pg_sync
from services.tenant_auth import generate_api_key

logger = logging.getLogger(__name__)
tenants_router = APIRouter(prefix="/api/v1/admin")


class TenantCreate(BaseModel):
    slug: str
    name: str
    plan: str = "free"
    key_name: Optional[str] = "default"


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
            # check duplicate
            cur.execute("SELECT id FROM tenants WHERE slug = %s", (slug,))
            if cur.fetchone():
                raise HTTPException(409, f"slug '{slug}' exists")
            # insert tenant (no RETURNING — fetch after)
            cur.execute(
                "INSERT INTO tenants (slug, name, plan) VALUES (%s, %s, %s)",
                (slug, body.name, body.plan)
            )
            # get the new tenant
            cur.execute("SELECT id, slug, name, plan, status FROM tenants WHERE slug = %s", (slug,))
            row = cur.fetchone()
            tenant_id = row[0]
            # insert api key
            cur.execute(
                "INSERT INTO api_keys (tenant_id, key_hash, name) VALUES (%s, %s, %s)",
                (tenant_id, key_hash, body.key_name)
            )
            conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"create_tenant: {e}")
        raise HTTPException(500, str(e))

    logger.info(f"tenant created: {slug} ({tenant_id})")
    base = f"https://{slug}.helix.millyweb.com"
    return {
        "id": tenant_id, "slug": row[1], "name": row[2],
        "plan": row[3], "status": row[4], "api_key": raw_key,
        "helix_url": base,
        "dashboard_url": f"{base}/dashboard",
        "setup_url": f"{base}/dashboard/home?key={raw_key}",
    }


@tenants_router.get("/tenants")
def list_tenants():
    try:
        with pg_sync.get_pg_conn(admin=True) as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, slug, name, plan, status, created_at FROM tenants ORDER BY created_at")
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(500, str(e))
    return [{"id": r[0], "slug": r[1], "name": r[2], "plan": r[3],
             "status": r[4], "created_at": str(r[5])} for r in rows]


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
