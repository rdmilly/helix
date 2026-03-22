"""Auth Service — shared helpers for all auth flows.

Handles:
  - JWT creation/verification (HTTP-only cookie)
  - Password hashing/verification
  - One-time token generation
  - Tenant auto-creation on first login
  - Cookie helpers (set/clear on .helix.millyweb.com)
"""
import hashlib
import logging
import os
import re
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Tuple

import bcrypt
import jwt as pyjwt
from fastapi import Request, HTTPException
from fastapi.responses import Response

log = logging.getLogger("helix.auth")

JWT_SECRET = os.environ.get("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = "HS256"
JWT_EXPIRE_DAYS = 7
COOKIE_NAME = "helix_session"
COOKIE_DOMAIN = os.environ.get("SESSION_COOKIE_DOMAIN", ".helix.millyweb.com")
COOKIE_SECURE = True
COOKIE_SAMESITE = "lax"


# ---------------------------------------------------------------------------
# JWT
# ---------------------------------------------------------------------------

def make_jwt(user_id: str, tenant_id: str, email: str, slug: str) -> str:
    payload = {
        "sub": user_id,
        "tid": tenant_id,
        "email": email,
        "slug": slug,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS),
    }
    return pyjwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)


def verify_jwt(token: str) -> Optional[Dict[str, Any]]:
    try:
        return pyjwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
    except pyjwt.ExpiredSignatureError:
        return None
    except pyjwt.InvalidTokenError:
        return None


def get_session(request: Request) -> Optional[Dict[str, Any]]:
    """Extract and verify JWT from cookie. Returns payload or None."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return None
    return verify_jwt(token)


def require_session(request: Request) -> Dict[str, Any]:
    """Like get_session but raises 401 if missing."""
    session = get_session(request)
    if not session:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return session


def set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        domain=COOKIE_DOMAIN,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
        max_age=60 * 60 * 24 * JWT_EXPIRE_DAYS,
        path="/",
    )


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(
        key=COOKIE_NAME,
        domain=COOKIE_DOMAIN,
        path="/",
    )


# ---------------------------------------------------------------------------
# Passwords
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt(12)).decode()


def verify_password(password: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode(), hashed.encode())
    except Exception:
        return False


# ---------------------------------------------------------------------------
# One-time tokens
# ---------------------------------------------------------------------------

def create_auth_token(user_id: str, token_type: str,
                      expires_minutes: int = 60) -> str:
    """Create a one-time token in auth_tokens table. Returns raw token."""
    from services.pg_sync import get_pg_conn
    token = secrets.token_urlsafe(32)
    expires = datetime.now(timezone.utc) + timedelta(minutes=expires_minutes)
    with get_pg_conn(admin=True) as conn:
        conn.execute(
            "INSERT INTO auth_tokens (token, user_id, type, expires_at) VALUES (%s, %s, %s, %s)",
            (token, user_id, token_type, expires)
        )
        conn.commit()
    return token


def consume_auth_token(token: str, expected_type: str) -> Optional[str]:
    """Validate and consume a one-time token. Returns user_id or None."""
    from services.pg_sync import get_pg_conn
    with get_pg_conn(admin=True) as conn:
        row = conn.execute(
            "SELECT user_id, expires_at, used_at FROM auth_tokens WHERE token=%s AND type=%s",
            (token, expected_type)
        ).fetchone()
        if not row:
            return None
        user_id, expires_at, used_at = row[0], row[1], row[2]
        if used_at:
            return None  # already used
        if datetime.now(timezone.utc) > expires_at:
            return None  # expired
        conn.execute(
            "UPDATE auth_tokens SET used_at=%s WHERE token=%s",
            (datetime.now(timezone.utc), token)
        )
        conn.commit()
    return user_id


# ---------------------------------------------------------------------------
# Tenant + user helpers
# ---------------------------------------------------------------------------

def _slugify(email: str) -> str:
    """Derive a URL-safe slug from email prefix."""
    prefix = email.split("@")[0].lower()
    slug = re.sub(r"[^a-z0-9]", "", prefix)[:20]
    return slug or "user"


def _unique_slug(base: str, conn) -> str:
    """Ensure slug is unique in tenants table."""
    slug = base
    i = 1
    while conn.execute("SELECT 1 FROM tenants WHERE slug=%s", (slug,)).fetchone():
        slug = f"{base}{i}"
        i += 1
    return slug


def get_or_create_user_google(
    google_id: str, email: str, name: str, avatar_url: str
) -> Tuple[str, str, str, bool]:
    """Find existing user by google_id, or create new user+tenant.
    Returns (user_id, tenant_id, slug, is_new).
    """
    from services.pg_sync import get_pg_conn
    with get_pg_conn(admin=True) as conn:
        # Check existing by google_id
        row = conn.execute(
            "SELECT u.id, t.id, t.slug FROM users u "
            "JOIN tenants t ON t.id = u.tenant_id "
            "WHERE u.google_id = %s",
            (google_id,)
        ).fetchone()
        if row:
            return row[0], row[1], row[2], False

        # Check existing by email (link account)
        row = conn.execute(
            "SELECT u.id, t.id, t.slug FROM users u "
            "JOIN tenants t ON t.id = u.tenant_id "
            "WHERE u.email = %s",
            (email,)
        ).fetchone()
        if row:
            # Link google_id to existing account
            conn.execute(
                "UPDATE users SET google_id=%s, avatar_url=%s, email_verified=TRUE WHERE id=%s",
                (google_id, avatar_url, row[0])
            )
            conn.commit()
            return row[0], row[1], row[2], False

        # New user — create tenant + user
        tenant_id = str(uuid.uuid4())
        user_id = str(uuid.uuid4())
        base_slug = _slugify(email)
        slug = _unique_slug(base_slug, conn)

        # Generate API key
        raw_key = "hx-" + secrets.token_urlsafe(32)
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

        conn.execute(
            "INSERT INTO tenants (id, slug, name, plan, created_at) "
            "VALUES (%s, %s, %s, 'free', NOW())",
            (tenant_id, slug, name or email)
        )
        conn.execute(
            "INSERT INTO api_keys (tenant_id, key_hash, name, created_at) "
            "VALUES (%s, %s, 'default', NOW())",
            (tenant_id, key_hash)
        )
        conn.execute(
            "INSERT INTO users (id, tenant_id, email, google_id, avatar_url, "
            "email_verified, password_hash, created_at) "
            "VALUES (%s, %s, %s, %s, %s, TRUE, '', NOW())",
            (user_id, tenant_id, email, google_id, avatar_url)
        )
        conn.commit()
        log.info(f"New user via Google: {email} tenant={slug}")
        return user_id, tenant_id, slug, True


def create_user_email(
    email: str, name: str, password: str
) -> Tuple[str, str, str, str]:
    """Create user+tenant via email/password registration.
    Returns (user_id, tenant_id, slug, raw_api_key).
    Raises ValueError if email already taken.
    """
    from services.pg_sync import get_pg_conn
    with get_pg_conn(admin=True) as conn:
        if conn.execute("SELECT 1 FROM users WHERE email=%s", (email,)).fetchone():
            raise ValueError("Email already registered")

        tenant_id = str(uuid.uuid4())
        user_id = str(uuid.uuid4())
        slug = _unique_slug(_slugify(email), conn)
        raw_key = "hx-" + secrets.token_urlsafe(32)
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        pw_hash = hash_password(password)

        conn.execute(
            "INSERT INTO tenants (id, slug, name, plan, created_at) "
            "VALUES (%s, %s, %s, 'free', NOW())",
            (tenant_id, slug, name or email)
        )
        conn.execute(
            "INSERT INTO api_keys (tenant_id, key_hash, name, created_at) "
            "VALUES (%s, %s, 'default', NOW())",
            (tenant_id, key_hash)
        )
        conn.execute(
            "INSERT INTO users (id, tenant_id, email, google_id, avatar_url, "
            "email_verified, password_hash, created_at) "
            "VALUES (%s, %s, %s, NULL, NULL, FALSE, %s, NOW())",
            (user_id, tenant_id, email, pw_hash)
        )
        conn.commit()
        log.info(f"New user via email: {email} tenant={slug}")
        return user_id, tenant_id, slug, raw_key


def login_user_email(email: str, password: str) -> Optional[Tuple[str, str, str]]:
    """Verify email/password. Returns (user_id, tenant_id, slug) or None."""
    from services.pg_sync import get_pg_conn
    with get_pg_conn(admin=True) as conn:
        row = conn.execute(
            "SELECT u.id, u.password_hash, t.id, t.slug "
            "FROM users u JOIN tenants t ON t.id = u.tenant_id "
            "WHERE u.email = %s",
            (email,)
        ).fetchone()
        if not row:
            return None
        user_id, pw_hash, tenant_id, slug = str(row[0]), str(row[1] or ''), str(row[2]), str(row[3])
        if not pw_hash or not verify_password(password, pw_hash):
            return None
        return user_id, tenant_id, slug
