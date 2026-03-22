"""Google OAuth 2.0 Router

GET /auth/google          — redirect to Google consent screen
GET /auth/google/callback — handle Google redirect, issue cookie, redirect to dashboard
"""
import logging
import os
import urllib.parse

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from services.auth_service import (
    get_or_create_user_google, make_jwt, set_session_cookie
)

log = logging.getLogger("helix.auth.google")

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI = os.environ.get(
    "GOOGLE_REDIRECT_URI",
    "https://helix.millyweb.com/auth/google/callback"
)

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"

auth_google_router = APIRouter()


@auth_google_router.get("/auth/google")
async def google_login(request: Request):
    """Redirect user to Google's OAuth consent screen."""
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "online",
        "prompt": "select_account",
    }
    url = GOOGLE_AUTH_URL + "?" + urllib.parse.urlencode(params)
    return RedirectResponse(url=url, status_code=302)


@auth_google_router.get("/auth/google/callback")
async def google_callback(request: Request, code: str = None, error: str = None):
    """Handle Google OAuth callback."""
    if error or not code:
        log.warning(f"Google OAuth error: {error}")
        return RedirectResponse(
            url="/login?error=google_denied",
            status_code=302
        )

    # Exchange code for tokens
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            token_resp = await client.post(GOOGLE_TOKEN_URL, data={
                "code": code,
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "redirect_uri": GOOGLE_REDIRECT_URI,
                "grant_type": "authorization_code",
            })
            token_resp.raise_for_status()
            tokens = token_resp.json()
        except Exception as e:
            log.error(f"Google token exchange failed: {e}")
            return RedirectResponse(url="/login?error=google_failed", status_code=302)

        # Get user profile
        try:
            info_resp = await client.get(
                GOOGLE_USERINFO_URL,
                headers={"Authorization": f"Bearer {tokens['access_token']}"}
            )
            info_resp.raise_for_status()
            profile = info_resp.json()
        except Exception as e:
            log.error(f"Google userinfo failed: {e}")
            return RedirectResponse(url="/login?error=google_failed", status_code=302)

    google_id = profile.get("sub", "")
    email = profile.get("email", "")
    name = profile.get("name", email.split("@")[0])
    avatar_url = profile.get("picture", "")

    if not google_id or not email:
        return RedirectResponse(url="/login?error=google_failed", status_code=302)

    try:
        user_id, tenant_id, slug, is_new = get_or_create_user_google(
            google_id, email, name, avatar_url
        )
    except Exception as e:
        log.error(f"User creation failed: {e}")
        return RedirectResponse(url="/login?error=server_error", status_code=302)

    # Issue JWT cookie and redirect to their dashboard
    jwt_token = make_jwt(user_id, tenant_id, email, slug)
    dashboard_url = f"https://{slug}.helix.millyweb.com/dashboard"

    response = RedirectResponse(url=dashboard_url, status_code=302)
    set_session_cookie(response, jwt_token)

    log.info(f"Google login: {email} → {slug} (new={is_new})")
    return response


@auth_google_router.get("/auth/logout")
async def logout(request: Request):
    """Clear session cookie and redirect to login."""
    from fastapi.responses import RedirectResponse
    from services.auth_service import clear_session_cookie
    response = RedirectResponse(url="/login", status_code=302)
    clear_session_cookie(response)
    return response
