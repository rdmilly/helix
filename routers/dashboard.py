"""
dashboard.py — Tenant-facing web dashboard

All auth via JWT cookie set on .helix.millyweb.com.
No API keys in URLs. No localStorage hacks.

GET /dashboard        — cookie check + redirect to /dashboard/home
GET /dashboard/home   — full dashboard (requires valid cookie)
GET /api/v1/dashboard — JSON data endpoint (cookie auth)
"""
import logging
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response

log = logging.getLogger("helix.dashboard")
dashboard_router = APIRouter()


# ---------------------------------------------------------------------------
# Redirect /dashboard → /dashboard/home (or login)
# ---------------------------------------------------------------------------

@dashboard_router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_redirect(request: Request):
    from services.auth_service import get_session
    session = get_session(request)
    if not session:
        return RedirectResponse(url="https://helix.millyweb.com/login", status_code=302)
    slug = session.get("slug", "")
    return RedirectResponse(
        url=f"https://{slug}.helix.millyweb.com/dashboard/home",
        status_code=302
    )


# ---------------------------------------------------------------------------
# JSON data endpoint — called by dashboard JS
# ---------------------------------------------------------------------------

@dashboard_router.get("/api/v1/dashboard", include_in_schema=False)
async def dashboard_data(request: Request):
    """Returns everything the dashboard needs in one call."""
    from services.auth_service import get_session
    from services.pg_sync import get_pg_conn

    session = get_session(request)
    if not session:
        return JSONResponse({"error": "unauthenticated"}, status_code=401)

    tenant_id = session.get("tid")
    email     = session.get("email", "")
    slug      = session.get("slug", "")

    try:
        with get_pg_conn(admin=True) as conn:
            # Tenant info + plan
            t = conn.execute(
                "SELECT slug, name, plan, created_at FROM tenants WHERE id=%s",
                (tenant_id,)
            ).fetchone()

            # API key (show masked, plus full for copy)
            k = conn.execute(
                "SELECT id FROM api_keys WHERE tenant_id=%s AND status='active' ORDER BY created_at LIMIT 1",
                (tenant_id,)
            ).fetchone()
            # We can't recover raw key (it's hashed) — show tenant's bearer token from JWT instead
            # The JWT itself IS the session credential. We expose it for MCP/extension config.

            # Usage stats (last 30 days)
            u = conn.execute("""
                SELECT
                    COUNT(DISTINCT session_id) AS sessions,
                    COALESCE(SUM(tokens_in),0) + COALESCE(SUM(tokens_out),0) AS tokens,
                    MAX(timestamp) AS last_seen
                FROM observer_session_tokens
                WHERE tenant_id=%s
                  AND timestamp >= NOW() - INTERVAL '30 days'
            """, (tenant_id,)).fetchone()

            # Connection status: has any data come in at all?
            first = conn.execute(
                "SELECT MIN(timestamp) FROM observer_session_tokens WHERE tenant_id=%s",
                (tenant_id,)
            ).fetchone()

    except Exception as e:
        log.error(f"dashboard_data error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

    connected = first and first[0] is not None

    return JSONResponse({
        "email": email,
        "slug": slug,
        "plan": t[2] if t else "free",
        "member_since": str(t[3])[:10] if t and t[3] else "",
        "connected": connected,
        "sessions": int(u[0]) if u and u[0] else 0,
        "tokens": int(u[1]) if u and u[1] else 0,
        "last_seen": str(u[2])[:16].replace("T"," ") if u and u[2] else None,
        "helix_url": f"https://{slug}.helix.millyweb.com",
    })


# ---------------------------------------------------------------------------
# Dashboard page
# ---------------------------------------------------------------------------

_DASH = r"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Helix Dashboard</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: #0a0a0f; color: #e2e8f0; min-height: 100vh;
    }
    /* Nav */
    nav {
      background: #13131a; border-bottom: 1px solid #1e1e2e;
      height: 56px; padding: 0 28px;
      display: flex; align-items: center; justify-content: space-between;
    }
    .logo { font-size: 18px; font-weight: 800; color: #7c3aed; letter-spacing: -0.5px; }
    .nav-right { display: flex; align-items: center; gap: 16px; }
    .user-pill {
      background: #1e1e2e; border-radius: 20px; padding: 5px 14px;
      font-size: 13px; color: #94a3b8;
    }
    .plan-badge {
      background: #7c3aed22; color: #a78bfa; border-radius: 4px;
      padding: 2px 8px; font-size: 11px; font-weight: 700; text-transform: uppercase;
    }
    .sign-out {
      font-size: 13px; color: #475569; text-decoration: none; cursor: pointer;
    }
    .sign-out:hover { color: #94a3b8; }
    /* Layout */
    main { max-width: 900px; margin: 0 auto; padding: 36px 24px; }
    h2 { font-size: 13px; font-weight: 600; color: #64748b; text-transform: uppercase;
         letter-spacing: .06em; margin-bottom: 14px; }
    /* Connection banner */
    .status-banner {
      border-radius: 12px; padding: 20px 24px;
      display: flex; align-items: center; gap: 16px; margin-bottom: 28px;
    }
    .status-banner.connected    { background: #0f1f13; border: 1px solid #166534; }
    .status-banner.disconnected { background: #1a1020; border: 1px solid #4c1d95; }
    .status-dot {
      width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0;
    }
    .status-dot.on  { background: #22c55e; box-shadow: 0 0 8px #22c55e66; }
    .status-dot.off { background: #7c3aed; }
    .status-text strong { display: block; font-size: 15px; margin-bottom: 2px; }
    .status-text span   { font-size: 13px; color: #94a3b8; }
    /* Stats row */
    .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; margin-bottom: 28px; }
    .stat {
      background: #13131a; border: 1px solid #1e1e2e; border-radius: 12px; padding: 20px;
    }
    .stat-label { font-size: 12px; color: #64748b; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; }
    .stat-val   { font-size: 30px; font-weight: 700; color: #f1f5f9; }
    .stat-sub   { font-size: 12px; color: #475569; margin-top: 4px; }
    /* Card */
    .card {
      background: #13131a; border: 1px solid #1e1e2e; border-radius: 12px;
      padding: 24px; margin-bottom: 20px;
    }
    .card-title { font-size: 15px; font-weight: 600; margin-bottom: 16px; }
    /* API key */
    .key-row {
      display: flex; align-items: center; gap: 10px;
      background: #0a0a0f; border: 1px solid #1e1e2e; border-radius: 8px;
      padding: 10px 14px;
    }
    .key-val {
      flex: 1; font-family: monospace; font-size: 14px; color: #a78bfa;
      overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }
    .copy-btn {
      flex-shrink: 0; background: #7c3aed; border: none; border-radius: 6px;
      padding: 6px 14px; color: #fff; font-size: 13px; font-weight: 600;
      cursor: pointer; transition: background .15s;
    }
    .copy-btn:hover { background: #6d28d9; }
    .copy-btn.copied { background: #16a34a; }
    .key-note { font-size: 12px; color: #475569; margin-top: 8px; }
    /* Install tabs */
    .install-tabs { display: flex; gap: 8px; margin-bottom: 16px; }
    .itab {
      padding: 7px 16px; border-radius: 8px; font-size: 13px; font-weight: 600;
      cursor: pointer; border: 1px solid #1e1e2e; color: #64748b;
      transition: all .15s;
    }
    .itab.active { background: #7c3aed; border-color: #7c3aed; color: #fff; }
    .ipanel { display: none; }
    .ipanel.active { display: block; }
    /* Steps */
    .steps { list-style: none; }
    .step {
      display: flex; gap: 14px; align-items: flex-start;
      padding: 12px 0; border-bottom: 1px solid #1e1e2e;
    }
    .step:last-child { border-bottom: none; }
    .step-num {
      width: 26px; height: 26px; border-radius: 50%; background: #1e1e2e;
      color: #a78bfa; font-size: 12px; font-weight: 700;
      display: flex; align-items: center; justify-content: center; flex-shrink: 0;
    }
    .step-body { flex: 1; }
    .step-title { font-size: 14px; font-weight: 600; margin-bottom: 4px; }
    .step-desc { font-size: 13px; color: #94a3b8; line-height: 1.5; }
    code {
      background: #0a0a0f; border: 1px solid #1e1e2e; border-radius: 4px;
      padding: 1px 6px; font-family: monospace; font-size: 12px; color: #7c3aed;
    }
    .code-block {
      background: #0a0a0f; border: 1px solid #1e1e2e; border-radius: 8px;
      padding: 12px 14px; font-family: monospace; font-size: 12px; color: #94a3b8;
      margin-top: 8px; white-space: pre-wrap; word-break: break-all;
      position: relative;
    }
    .code-copy {
      position: absolute; top: 8px; right: 8px;
      background: #1e1e2e; border: none; border-radius: 4px;
      padding: 3px 8px; color: #94a3b8; font-size: 11px; cursor: pointer;
    }
    .code-copy:hover { background: #2d2d3e; }
    a.link { color: #7c3aed; text-decoration: none; }
    a.link:hover { text-decoration: underline; }
    #loading { text-align: center; padding: 80px; color: #475569; font-size: 15px; }
  </style>
</head>
<body>

<nav>
  <div class="logo">⧠ Helix</div>
  <div class="nav-right">
    <span class="plan-badge" id="plan">free</span>
    <span class="user-pill" id="user-email">loading...</span>
    <a class="sign-out" href="/auth/logout">Sign out</a>
  </div>
</nav>

<main id="main" style="display:none">

  <!-- Connection status banner -->
  <div class="status-banner" id="status-banner">
    <div class="status-dot" id="status-dot"></div>
    <div class="status-text">
      <strong id="status-title"></strong>
      <span id="status-desc"></span>
    </div>
  </div>

  <!-- Stats -->
  <h2>Last 30 days</h2>
  <div class="stats">
    <div class="stat">
      <div class="stat-label">Sessions</div>
      <div class="stat-val" id="stat-sessions">0</div>
      <div class="stat-sub">captured conversations</div>
    </div>
    <div class="stat">
      <div class="stat-label">Tokens</div>
      <div class="stat-val" id="stat-tokens">0</div>
      <div class="stat-sub">processed through Helix</div>
    </div>
    <div class="stat">
      <div class="stat-label">Last activity</div>
      <div class="stat-val" style="font-size:16px;padding-top:8px" id="stat-last">&mdash;</div>
      <div class="stat-sub" id="stat-since"></div>
    </div>
  </div>

  <!-- API Key -->
  <h2>Your connection key</h2>
  <div class="card">
    <div class="card-title">API Key</div>
    <div class="key-row">
      <div class="key-val" id="api-key-display"></div>
      <button class="copy-btn" id="copy-key-btn" onclick="copyKey()">Copy</button>
    </div>
    <div class="key-note">Paste this into the MemBrain extension or your Claude Desktop config. Keep it private.</div>
  </div>

  <!-- Install instructions -->
  <h2>Connect a device</h2>
  <div class="card">
    <div class="install-tabs">
      <div class="itab active" id="tab-ext" onclick="switchInstall('ext')">MemBrain Extension</div>
      <div class="itab" id="tab-mcp" onclick="switchInstall('mcp')">Claude Desktop MCP</div>
    </div>

    <!-- Extension panel -->
    <div class="ipanel active" id="panel-ext">
      <ol class="steps">
        <li class="step">
          <div class="step-num">1</div>
          <div class="step-body">
            <div class="step-title">Install the MemBrain extension</div>
            <div class="step-desc">Add it from the Chrome Web Store. It works in Chrome, Edge, and Brave.</div>
          </div>
        </li>
        <li class="step">
          <div class="step-num">2</div>
          <div class="step-body">
            <div class="step-title">Open extension options</div>
            <div class="step-desc">Click the MemBrain icon → Options. Paste your API key and set the Helix endpoint.</div>
          </div>
        </li>
        <li class="step">
          <div class="step-num">3</div>
          <div class="step-body">
            <div class="step-title">Set your Helix endpoint</div>
            <div class="step-desc">
              Endpoint URL:
              <div class="code-block" id="endpoint-url"></div>
            </div>
          </div>
        </li>
        <li class="step">
          <div class="step-num">4</div>
          <div class="step-body">
            <div class="step-title">Start a Claude conversation</div>
            <div class="step-desc">Open claude.ai and start chatting. Your sessions will appear in this dashboard within a minute.</div>
          </div>
        </li>
      </ol>
    </div>

    <!-- MCP panel -->
    <div class="ipanel" id="panel-mcp">
      <ol class="steps">
        <li class="step">
          <div class="step-num">1</div>
          <div class="step-body">
            <div class="step-title">Open your Claude Desktop config</div>
            <div class="step-desc">
              Mac: <code>~/Library/Application Support/Claude/claude_desktop_config.json</code><br>
              Windows: <code>%APPDATA%\Claude\claude_desktop_config.json</code>
            </div>
          </div>
        </li>
        <li class="step">
          <div class="step-num">2</div>
          <div class="step-body">
            <div class="step-title">Add the Helix MCP server</div>
            <div class="step-desc">Paste this inside the <code>mcpServers</code> object:
              <div class="code-block" id="mcp-snippet" style="position:relative">
                <button class="code-copy" onclick="copySnippet()">Copy</button>
                <span id="mcp-snippet-text"></span>
              </div>
            </div>
          </div>
        </li>
        <li class="step">
          <div class="step-num">3</div>
          <div class="step-body">
            <div class="step-title">Restart Claude Desktop</div>
            <div class="step-desc">Claude will connect to Helix on next launch. You'll see the Helix tools appear in the tool list.</div>
          </div>
        </li>
      </ol>
    </div>
  </div>

</main>

<div id="loading">⧠ Loading your dashboard…</div>

<script>
  let _apiKey = '';
  let _helixUrl = '';

  function fmt(n) {
    if (n >= 1000000) return (n/1000000).toFixed(1) + 'M';
    if (n >= 1000) return (n/1000).toFixed(1) + 'k';
    return String(n);
  }

  async function load() {
    try {
      const r = await fetch('/api/v1/dashboard', {credentials: 'include'});
      if (r.status === 401) {
        window.location.href = 'https://helix.millyweb.com/login';
        return;
      }
      const d = await r.json();

      // Nav
      document.getElementById('user-email').textContent = d.email;
      document.getElementById('plan').textContent = d.plan;

      // API key = JWT token (bearer for MCP/extension calls)
      // We re-issue a long-lived key by making an authenticated request
      const keyR = await fetch('/api/v1/init/my-key', {credentials:'include'});
      if (keyR.ok) {
        const keyD = await keyR.json();
        _apiKey = keyD.api_key || '';
      }
      if (!_apiKey) {
        // Fallback: show placeholder until key endpoint is ready
        _apiKey = 'hx-•••• (rotate keys to see)';
      }

      document.getElementById('api-key-display').textContent = _apiKey;
      _helixUrl = d.helix_url;

      // Endpoint + MCP snippet
      document.getElementById('endpoint-url').textContent = d.helix_url;
      document.getElementById('mcp-snippet-text').textContent =
        '"helix": {\n  "command": "npx",\n  "args": ["-y", "@helix/mcp-client"],\n  "env": {\n    "HELIX_URL": "' + d.helix_url + '",\n    "HELIX_KEY": "' + _apiKey + '"\n  }\n}';

      // Status banner
      const banner = document.getElementById('status-banner');
      const dot    = document.getElementById('status-dot');
      if (d.connected) {
        banner.className = 'status-banner connected';
        dot.className = 'status-dot on';
        document.getElementById('status-title').textContent = 'Helix is active';
        document.getElementById('status-desc').textContent =
          d.last_seen ? 'Last session: ' + d.last_seen : 'Capturing your sessions';
      } else {
        banner.className = 'status-banner disconnected';
        dot.className = 'status-dot off';
        document.getElementById('status-title').textContent = 'Not connected yet';
        document.getElementById('status-desc').textContent =
          'Install the extension or MCP below to start capturing sessions';
      }

      // Stats
      document.getElementById('stat-sessions').textContent = fmt(d.sessions);
      document.getElementById('stat-tokens').textContent   = fmt(d.tokens);
      if (d.last_seen) {
        document.getElementById('stat-last').textContent  = d.last_seen.slice(0,10);
      }
      if (d.member_since) {
        document.getElementById('stat-since').textContent = 'member since ' + d.member_since;
      }

      document.getElementById('loading').style.display = 'none';
      document.getElementById('main').style.display = 'block';

    } catch(e) {
      document.getElementById('loading').textContent = 'Error loading dashboard. Try refreshing.';
    }
  }

  function copyKey() {
    navigator.clipboard.writeText(_apiKey).then(() => {
      const btn = document.getElementById('copy-key-btn');
      btn.textContent = 'Copied!';
      btn.classList.add('copied');
      setTimeout(() => { btn.textContent = 'Copy'; btn.classList.remove('copied'); }, 2000);
    });
  }

  function copySnippet() {
    const text = document.getElementById('mcp-snippet-text').textContent;
    navigator.clipboard.writeText(text).then(() => {
      const btn = document.querySelector('.code-copy');
      btn.textContent = 'Copied!';
      setTimeout(() => btn.textContent = 'Copy', 2000);
    });
  }

  function switchInstall(tab) {
    document.querySelectorAll('.itab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.ipanel').forEach(p => p.classList.remove('active'));
    document.getElementById('tab-' + tab).classList.add('active');
    document.getElementById('panel-' + tab).classList.add('active');
  }

  // Refresh stats every 60s while page is open
  load();
  setInterval(() => { if (!document.hidden) load(); }, 60000);
</script>
</body>
</html>
"""


@dashboard_router.get("/dashboard/home", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_home(request: Request):
    """Serve dashboard. Redirects to login if no cookie."""
    from services.auth_service import get_session
    session = get_session(request)
    if not session:
        return RedirectResponse(url="https://helix.millyweb.com/login", status_code=302)
    return HTMLResponse(_DASH)
