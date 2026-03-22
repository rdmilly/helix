"""Login / Signup page router.

GET /login   — serve the login/signup page
GET /        — redirect logged-in users to dashboard, else /login
"""
import os
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from services.auth_service import get_session

login_router = APIRouter()

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")

_LOGIN_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Helix — Sign In</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: #0a0a0f;
      color: #e2e8f0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .card {
      background: #13131a;
      border: 1px solid #1e1e2e;
      border-radius: 16px;
      padding: 40px;
      width: 100%;
      max-width: 440px;
      box-shadow: 0 24px 64px rgba(0,0,0,.5);
    }
    .logo {
      font-size: 26px;
      font-weight: 800;
      color: #7c3aed;
      letter-spacing: -0.5px;
      margin-bottom: 6px;
    }
    .tagline { color: #64748b; font-size: 14px; margin-bottom: 32px; }
    /* Tabs */
    .tabs { display: flex; gap: 0; margin-bottom: 28px; border-bottom: 1px solid #1e1e2e; }
    .tab {
      flex: 1; padding: 10px; text-align: center; font-size: 14px; font-weight: 600;
      color: #64748b; cursor: pointer; border-bottom: 2px solid transparent;
      transition: all .15s; user-select: none;
    }
    .tab.active { color: #a78bfa; border-bottom-color: #7c3aed; }
    /* Google button */
    .btn-google {
      width: 100%; display: flex; align-items: center; justify-content: center; gap: 10px;
      background: #fff; color: #1a1a1a; border: none; border-radius: 10px;
      padding: 12px 16px; font-size: 15px; font-weight: 600; cursor: pointer;
      transition: background .15s; text-decoration: none;
    }
    .btn-google:hover { background: #f1f5f9; }
    .btn-google svg { width: 20px; height: 20px; flex-shrink: 0; }
    .divider {
      display: flex; align-items: center; gap: 12px;
      margin: 24px 0; color: #334155; font-size: 13px;
    }
    .divider::before, .divider::after {
      content: ''; flex: 1; height: 1px; background: #1e1e2e;
    }
    /* Form */
    .form-group { margin-bottom: 16px; }
    label { display: block; font-size: 13px; color: #94a3b8; margin-bottom: 6px; font-weight: 500; }
    input[type=email], input[type=password], input[type=text] {
      width: 100%; background: #0a0a0f; border: 1px solid #1e1e2e;
      border-radius: 8px; padding: 11px 14px; color: #e2e8f0;
      font-size: 14px; outline: none; transition: border-color .15s;
    }
    input:focus { border-color: #7c3aed; }
    .btn-primary {
      width: 100%; background: #7c3aed; border: none; border-radius: 10px;
      padding: 12px; color: #fff; font-size: 15px; font-weight: 600;
      cursor: pointer; transition: background .15s; margin-top: 4px;
    }
    .btn-primary:hover { background: #6d28d9; }
    .forgot { font-size: 13px; color: #64748b; margin-top: 12px; text-align: right; }
    .forgot a { color: #7c3aed; text-decoration: none; }
    .forgot a:hover { text-decoration: underline; }
    /* Error / message banners */
    .banner {
      border-radius: 8px; padding: 12px 14px; font-size: 13px;
      margin-bottom: 20px; display: none;
    }
    .banner.error { background: #1f1315; border: 1px solid #7f1d1d; color: #fca5a5; }
    .banner.info  { background: #0f1f13; border: 1px solid #14532d; color: #86efac; }
    .panel { display: none; }
    .panel.active { display: block; }
    .terms { font-size: 12px; color: #475569; margin-top: 16px; text-align: center; }
  </style>
</head>
<body>
<div class="card">
  <div class="logo">⧠ Helix</div>
  <div class="tagline">AI memory that compounds with every session</div>

  <div id="banner" class="banner"></div>

  <div class="tabs">
    <div class="tab" id="tab-login" onclick="switchTab('login')">Sign In</div>
    <div class="tab" id="tab-signup" onclick="switchTab('signup')">Create Account</div>
  </div>

  <!-- Login panel -->
  <div class="panel" id="panel-login">
    <a href="/auth/google" class="btn-google">
      <svg viewBox="0 0 24 24"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.84z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg>
      Continue with Google
    </a>

    <div class="divider">or continue with email</div>

    <form method="post" action="/auth/login">
      <div class="form-group">
        <label>Email</label>
        <input type="email" name="email" placeholder="you@example.com" required autocomplete="email">
      </div>
      <div class="form-group">
        <label>Password</label>
        <input type="password" name="password" placeholder="Your password" required autocomplete="current-password">
      </div>
      <button type="submit" class="btn-primary">Sign In</button>
      <div class="forgot"><a href="#" onclick="showForgot()">Forgot password?</a></div>
    </form>
  </div>

  <!-- Signup panel -->
  <div class="panel" id="panel-signup">
    <a href="/auth/google" class="btn-google">
      <svg viewBox="0 0 24 24"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.84z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg>
      Sign up with Google
    </a>

    <div class="divider">or sign up with email</div>

    <form method="post" action="/auth/signup">
      <div class="form-group">
        <label>Full Name</label>
        <input type="text" name="name" placeholder="Jane Smith" required autocomplete="name">
      </div>
      <div class="form-group">
        <label>Email</label>
        <input type="email" name="email" placeholder="you@example.com" required autocomplete="email">
      </div>
      <div class="form-group">
        <label>Password</label>
        <input type="password" name="password" placeholder="8+ characters" required minlength="8" autocomplete="new-password">
      </div>
      <button type="submit" class="btn-primary">Create Account</button>
      <div class="terms">By signing up you agree to our Terms of Service</div>
    </form>
  </div>

  <!-- Forgot password (hidden by default) -->
  <div class="panel" id="panel-forgot">
    <p style="color:#94a3b8;font-size:14px;margin-bottom:20px">Enter your email and we'll send a reset link.</p>
    <form method="post" action="/auth/forgot">
      <div class="form-group">
        <label>Email</label>
        <input type="email" name="email" placeholder="you@example.com" required>
      </div>
      <button type="submit" class="btn-primary">Send Reset Link</button>
    </form>
    <div style="margin-top:16px;text-align:center">
      <a href="#" onclick="switchTab('login')" style="color:#7c3aed;font-size:13px;text-decoration:none">← Back to sign in</a>
    </div>
  </div>

</div>

<script>
  const ERRORS = {
    invalid_credentials: 'Incorrect email or password.',
    email_taken: 'An account with this email already exists. Try signing in.',
    password_too_short: 'Password must be at least 8 characters.',
    google_denied: 'Google sign-in was cancelled.',
    google_failed: 'Google sign-in failed. Please try again.',
    server_error: 'Something went wrong. Please try again.',
    invalid_reset_link: 'This reset link is invalid or expired.',
    invalid_verify_link: 'This verification link is invalid or expired.',
  };
  const MESSAGES = {
    reset_sent: 'Check your email for a password reset link.',
    password_reset: 'Password updated successfully. Sign in below.',
    email_verified: 'Email verified! Welcome to Helix.',
  };

  function switchTab(tab) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    document.getElementById('tab-' + tab).classList.add('active');
    document.getElementById('panel-' + tab).classList.add('active');
  }

  function showForgot() {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    document.getElementById('panel-forgot').classList.add('active');
  }

  function showBanner(msg, type) {
    const b = document.getElementById('banner');
    b.textContent = msg;
    b.className = 'banner ' + type;
    b.style.display = 'block';
  }

  // Read URL params on load
  (function() {
    const params = new URLSearchParams(window.location.search);
    const tab = params.get('tab') || 'login';
    switchTab(tab);

    const err = params.get('error');
    if (err && ERRORS[err]) showBanner(ERRORS[err], 'error');

    const msg = params.get('message');
    if (msg && MESSAGES[msg]) showBanner(MESSAGES[msg], 'info');
  })();
</script>
</body>
</html>
"""


@login_router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Serve the login/signup page. Redirect to dashboard if already logged in."""
    session = get_session(request)
    if session:
        slug = session.get("slug", "")
        return RedirectResponse(
            url=f"https://{slug}.helix.millyweb.com/dashboard",
            status_code=302
        )
    return HTMLResponse(_LOGIN_PAGE)


@login_router.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Root redirect."""
    session = get_session(request)
    if session:
        slug = session.get("slug", "")
        return RedirectResponse(
            url=f"https://{slug}.helix.millyweb.com/dashboard",
            status_code=302
        )
    return RedirectResponse(url="/login", status_code=302)
