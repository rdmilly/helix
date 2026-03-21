"""
auth.py — User auth: register, login, JWT

POST /api/v1/auth/register  — create user + tenant, send invite
POST /api/v1/auth/login     — email + password → JWT
GET  /api/v1/auth/me        — verify JWT, return user info
POST /api/v1/auth/logout    — (client-side: just discard token)
"""
import os
import logging
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional

# bcrypt and jwt imported lazily at call time
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from services import pg_sync
from services.tenant_auth import generate_api_key
from services.email import send_invite

logger = logging.getLogger(__name__)
auth_router = APIRouter(prefix="/api/v1/auth")

JWT_SECRET = os.environ.get("HELIX_JWT_SECRET", "helix-dev-secret-change-in-prod")
JWT_ALGO   = "HS256"
JWT_TTL_H  = 24 * 7  # 7 days


class RegisterRequest(BaseModel):
    email: str
    password: str
    name: Optional[str] = None        # display name
    slug: Optional[str] = None        # tenant slug; derived from email if omitted
    plan: str = "free"


class LoginRequest(BaseModel):
    email: str
    password: str


def _slug_from_email(email: str) -> str:
    base = email.split("@")[0].lower()
    slug = "".join(c for c in base if c.isalnum() or c in "-_")
    return slug[:30] or "user"


def _make_token(user_id: str, tenant_id: str, email: str) -> str:
    import jwt
    payload = {
        "sub": user_id,
        "tenant_id": tenant_id,
        "email": email,
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_TTL_H),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)


def _verify_token(token: str) -> dict:
    import jwt
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "token expired")
    except Exception:
        raise HTTPException(401, "invalid token")


@auth_router.post("/register")
def register(body: RegisterRequest):
    email = body.email.strip().lower()
    if not email or "@" not in email:
        raise HTTPException(400, "invalid email")
    if len(body.password) < 8:
        raise HTTPException(400, "password must be 8+ chars")

    slug = (body.slug or _slug_from_email(email)).lower().strip()
    name = body.name or slug
    import bcrypt
    pw_hash = bcrypt.hashpw(body.password.encode(), bcrypt.gensalt()).decode()
    raw_key, key_hash = generate_api_key()

    try:
        with pg_sync.get_pg_conn(admin=True) as conn:
            cur = conn.cursor()

            # Check email not taken
            cur.execute("SELECT id FROM users WHERE email = %s", (email,))
            if cur.fetchone():
                raise HTTPException(409, "email already registered")

            # Ensure unique slug
            base_slug, n = slug, 1
            while True:
                cur.execute("SELECT id FROM tenants WHERE slug = %s", (slug,))
                if not cur.fetchone():
                    break
                slug = f"{base_slug}{n}"
                n += 1

            # Create tenant
            cur.execute(
                "INSERT INTO tenants (slug, name, plan, meta) VALUES (%s, %s, %s, %s)",
                (slug, name, body.plan, f'{{"email":"{email}"}}')
            )
            cur.execute("SELECT id FROM tenants WHERE slug = %s", (slug,))
            tenant_id = cur.fetchone()[0]

            # Create API key for machines
            cur.execute(
                "INSERT INTO api_keys (tenant_id, key_hash, name) VALUES (%s, %s, %s)",
                (tenant_id, key_hash, "default")
            )

            # Create user
            cur.execute(
                "INSERT INTO users (tenant_id, email, password_hash, role) VALUES (%s, %s, %s, %s) RETURNING id",
                (tenant_id, email, pw_hash, "owner")
            )
            row = cur.fetchone()
            # RETURNING consumed by PgCursor execute; re-fetch
            cur.execute("SELECT id FROM users WHERE email = %s", (email,))
            user_id = cur.fetchone()[0]
            conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"register error: {e}")
        raise HTTPException(500, str(e))

    base = f"https://{slug}.helix.millyweb.com"
    token = _make_token(user_id, tenant_id, email)

    # Send welcome email
    setup_url = f"{base}/dashboard"
    try:
        send_invite(email, name, slug, setup_url)
    except Exception as e:
        logger.warning(f"invite email failed: {e}")

    logger.info(f"registered: {email} tenant={slug}")
    return {
        "token": token,
        "user_id": user_id,
        "tenant_id": tenant_id,
        "tenant": slug,
        "email": email,
        "helix_url": base,
        "dashboard_url": f"{base}/dashboard",
        "api_key": raw_key,
    }


@auth_router.post("/login")
def login(body: LoginRequest):
    email = body.email.strip().lower()
    try:
        with pg_sync.get_pg_conn(admin=True) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, tenant_id, password_hash FROM users WHERE email = %s",
                (email,)
            )
            row = cur.fetchone()
    except Exception as e:
        raise HTTPException(500, str(e))

    if not row:
        raise HTTPException(401, "invalid credentials")

    user_id, tenant_id, pw_hash = row[0], row[1], row[2]
    import bcrypt
    if not bcrypt.checkpw(body.password.encode(), pw_hash.encode()):
        raise HTTPException(401, "invalid credentials")

    token = _make_token(user_id, tenant_id, email)
    logger.info(f"login: {email}")
    return {"token": token, "user_id": user_id, "tenant_id": tenant_id, "email": email}


@auth_router.get("/me")
def me(request: Request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(401, "missing token")
    payload = _verify_token(auth[7:])
    return {
        "user_id": payload["sub"],
        "tenant_id": payload["tenant_id"],
        "email": payload["email"],
    }
