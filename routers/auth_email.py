"""Email + Password Auth Router

POST /auth/signup   — register new user
POST /auth/login    — login with email+password
POST /auth/forgot   — send password reset email
GET  /auth/reset    — consume reset token, show new password form
POST /auth/reset    — set new password
GET  /auth/verify   — verify email from magic link
"""
import logging
import os
from typing import Optional

from fastapi import APIRouter, Request, Form
from fastapi.responses import RedirectResponse, HTMLResponse

from services.auth_service import (
    create_user_email, login_user_email, make_jwt,
    set_session_cookie, create_auth_token, consume_auth_token,
    hash_password
)

log = logging.getLogger("helix.auth.email")

auth_email_router = APIRouter()


def _send_verify_email(email: str, name: str, token: str):
    """Send email verification link via Resend."""
    try:
        from services.email import send_email
        url = f"https://helix.millyweb.com/auth/verify?token={token}"
        send_email(
            to=email,
            subject="Verify your Helix account",
            body=f"Hi {name},\n\nClick the link to verify your email:\n{url}\n\nExpires in 24 hours.",
        )
    except Exception as e:
        log.warning(f"Verify email send failed: {e}")


def _send_reset_email(email: str, token: str):
    """Send password reset link via Resend."""
    try:
        from services.email import send_email
        url = f"https://helix.millyweb.com/auth/reset?token={token}"
        send_email(
            to=email,
            subject="Reset your Helix password",
            body=f"Click the link to reset your password:\n{url}\n\nExpires in 1 hour. Ignore this if you didn't request it.",
        )
    except Exception as e:
        log.warning(f"Reset email send failed: {e}")


@auth_email_router.post("/auth/signup")
async def signup(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    name: str = Form(...),
):
    email = email.strip().lower()
    name = name.strip()

    if len(password) < 8:
        return RedirectResponse(
            url="/login?tab=signup&error=password_too_short",
            status_code=302
        )

    try:
        user_id, tenant_id, slug, raw_key = create_user_email(email, name, password)
    except ValueError:
        return RedirectResponse(
            url="/login?tab=signup&error=email_taken",
            status_code=302
        )
    except Exception as e:
        log.error(f"Signup error: {e}")
        return RedirectResponse(
            url="/login?tab=signup&error=server_error",
            status_code=302
        )

    # Send verification email (non-blocking)
    token = create_auth_token(user_id, "verify_email", expires_minutes=60*24)
    _send_verify_email(email, name, token)

    # Issue session and redirect to dashboard
    jwt_token = make_jwt(user_id, tenant_id, email, slug)
    dashboard_url = f"https://{slug}.helix.millyweb.com/dashboard"
    response = RedirectResponse(url=dashboard_url, status_code=302)
    set_session_cookie(response, jwt_token)
    log.info(f"Signup: {email} → {slug}")
    return response


@auth_email_router.post("/auth/login")
async def login(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
):
    email = email.strip().lower()
    result = login_user_email(email, password)
    if not result:
        return RedirectResponse(
            url="/login?error=invalid_credentials",
            status_code=302
        )

    user_id, tenant_id, slug = result
    jwt_token = make_jwt(user_id, tenant_id, email, slug)
    dashboard_url = f"https://{slug}.helix.millyweb.com/dashboard"
    response = RedirectResponse(url=dashboard_url, status_code=302)
    set_session_cookie(response, jwt_token)
    log.info(f"Login: {email} → {slug}")
    return response


@auth_email_router.post("/auth/forgot")
async def forgot_password(request: Request, email: str = Form(...)):
    email = email.strip().lower()
    # Always return success to avoid email enumeration
    try:
        from services.pg_sync import get_pg_conn
        with get_pg_conn(admin=True) as conn:
            row = conn.execute(
                "SELECT id FROM users WHERE email=%s", (email,)
            ).fetchone()
        if row:
            token = create_auth_token(row[0], "reset_password", expires_minutes=60)
            _send_reset_email(email, token)
    except Exception as e:
        log.warning(f"Forgot password error: {e}")
    return RedirectResponse(
        url="/login?message=reset_sent",
        status_code=302
    )


@auth_email_router.get("/auth/reset")
async def reset_form(request: Request, token: str = ""):
    """Show new password form."""
    # Validate token exists before showing form
    from services.pg_sync import get_pg_conn
    try:
        with get_pg_conn(admin=True) as conn:
            row = conn.execute(
                "SELECT user_id FROM auth_tokens WHERE token=%s AND type='reset_password' AND used_at IS NULL",
                (token,)
            ).fetchone()
    except Exception:
        row = None

    if not row:
        return RedirectResponse(url="/login?error=invalid_reset_link", status_code=302)

    html = f"""<!DOCTYPE html><html><head><title>Reset Password — Helix</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0a0a0f;color:#e2e8f0;min-height:100vh;display:flex;align-items:center;justify-content:center}}
.card{{background:#13131a;border:1px solid #1e1e2e;border-radius:12px;padding:40px;width:100%;max-width:420px}}
.logo{{font-size:22px;font-weight:700;color:#7c3aed;margin-bottom:24px}}
label{{display:block;font-size:13px;color:#94a3b8;margin-bottom:6px;margin-top:16px}}
input{{width:100%;background:#0a0a0f;border:1px solid #1e1e2e;border-radius:8px;padding:12px;color:#e2e8f0;font-size:14px;outline:none}}
input:focus{{border-color:#7c3aed}}
button{{width:100%;margin-top:20px;background:#7c3aed;border:none;border-radius:8px;padding:12px;color:#fff;font-size:15px;font-weight:600;cursor:pointer}}
</style></head><body>
<div class="card">
<div class="logo">⧠ Helix</div>
<h2 style="font-size:18px;margin-bottom:4px">Set new password</h2>
<form method="post" action="/auth/reset">
  <input type="hidden" name="token" value="{token}">
  <label>New password</label>
  <input type="password" name="password" placeholder="8+ characters" required minlength="8">
  <label>Confirm password</label>
  <input type="password" name="confirm" placeholder="Repeat password" required>
  <button type="submit">Set Password</button>
</form>
</div></body></html>"""
    return HTMLResponse(html)


@auth_email_router.post("/auth/reset")
async def reset_password(
    request: Request,
    token: str = Form(...),
    password: str = Form(...),
    confirm: str = Form(...),
):
    if password != confirm or len(password) < 8:
        return RedirectResponse(
            url=f"/auth/reset?token={token}&error=password_mismatch",
            status_code=302
        )

    user_id = consume_auth_token(token, "reset_password")
    if not user_id:
        return RedirectResponse(url="/login?error=invalid_reset_link", status_code=302)

    try:
        from services.pg_sync import get_pg_conn
        pw_hash = hash_password(password)
        with get_pg_conn(admin=True) as conn:
            conn.execute(
                "UPDATE users SET password_hash=%s WHERE id=%s",
                (pw_hash, user_id)
            )
            conn.commit()
    except Exception as e:
        log.error(f"Password reset error: {e}")
        return RedirectResponse(url="/login?error=server_error", status_code=302)

    return RedirectResponse(url="/login?message=password_reset", status_code=302)


@auth_email_router.get("/auth/verify")
async def verify_email(request: Request, token: str = ""):
    user_id = consume_auth_token(token, "verify_email")
    if not user_id:
        return RedirectResponse(url="/login?error=invalid_verify_link", status_code=302)
    try:
        from services.pg_sync import get_pg_conn
        with get_pg_conn(admin=True) as conn:
            conn.execute(
                "UPDATE users SET email_verified=TRUE WHERE id=%s", (user_id,)
            )
            conn.commit()
    except Exception:
        pass
    return RedirectResponse(url="/dashboard?message=email_verified", status_code=302)
