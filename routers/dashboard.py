"""
dashboard.py — Layer 6d: tenant-facing web dashboard

GET  /dashboard        — login page (enter API key)
GET  /dashboard/home   — main dashboard (requires ?key=hx-...)
"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse

dashboard_router = APIRouter()

_LOGIN_HTML = """
<!DOCTYPE html><html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Helix — Sign In</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0a0a0f;color:#e2e8f0;min-height:100vh;display:flex;align-items:center;justify-content:center}
  .card{background:#13131a;border:1px solid #1e1e2e;border-radius:12px;padding:40px;width:100%;max-width:420px}
  .logo{font-size:24px;font-weight:700;color:#7c3aed;margin-bottom:8px}  
  .sub{color:#64748b;font-size:14px;margin-bottom:32px}
  label{display:block;font-size:13px;color:#94a3b8;margin-bottom:6px}
  input{width:100%;background:#0a0a0f;border:1px solid #1e1e2e;border-radius:8px;padding:12px 14px;color:#e2e8f0;font-size:14px;font-family:monospace;outline:none}
  input:focus{border-color:#7c3aed}
  button{width:100%;margin-top:16px;background:#7c3aed;border:none;border-radius:8px;padding:12px;color:#fff;font-size:15px;font-weight:600;cursor:pointer}
  button:hover{background:#6d28d9}
  .err{color:#f87171;font-size:13px;margin-top:12px;display:none}
</style></head>
<body>
<div class="card">
  <div class="logo">⧠ Helix</div>
  <div class="sub">Enter your API key to view your dashboard</div>
  <label>API Key</label>
  <input id="key" type="password" placeholder="hx-..." autocomplete="off">
  <button onclick="go()">Sign In</button>
  <div class="err" id="err">Invalid key — check and try again</div>
</div>
<script>
  document.getElementById('key').addEventListener('keydown', e => e.key==='Enter' && go())
  function go(){
    const k = document.getElementById('key').value.trim()
    if(!k){return}
    window.location.href = '/dashboard/home?key=' + encodeURIComponent(k)
  }
</script></body></html>
"""

_DASH_HTML = """
<!DOCTYPE html><html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Helix Dashboard</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0a0a0f;color:#e2e8f0;min-height:100vh}
  .nav{background:#13131a;border-bottom:1px solid #1e1e2e;padding:0 24px;height:56px;display:flex;align-items:center;justify-content:space-between}
  .logo{font-size:18px;font-weight:700;color:#7c3aed}
  .tenant-badge{background:#1e1e2e;border-radius:6px;padding:4px 12px;font-size:13px;color:#94a3b8}
  .plan-badge{background:#7c3aed22;color:#a78bfa;border-radius:4px;padding:2px 8px;font-size:11px;font-weight:600;text-transform:uppercase;margin-left:8px}
  main{max-width:1100px;margin:0 auto;padding:32px 24px}
  h2{font-size:20px;font-weight:600;margin-bottom:20px;color:#f1f5f9}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:32px}
  .stat{background:#13131a;border:1px solid #1e1e2e;border-radius:10px;padding:20px}
  .stat-label{font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px}
  .stat-val{font-size:28px;font-weight:700;color:#f1f5f9}
  .stat-sub{font-size:12px;color:#475569;margin-top:4px}
  .accent{color:#7c3aed}
  .section{background:#13131a;border:1px solid #1e1e2e;border-radius:10px;padding:24px;margin-bottom:24px}
  table{width:100%;border-collapse:collapse}
  th{text-align:left;font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.05em;padding:8px 12px;border-bottom:1px solid #1e1e2e}
  td{padding:10px 12px;font-size:14px;border-bottom:1px solid #0f0f17}
  tr:last-child td{border-bottom:none}
  .empty{color:#475569;font-size:14px;padding:20px 0;text-align:center}
  .bar-track{background:#1e1e2e;border-radius:4px;height:6px;margin-top:8px}
  .bar-fill{background:#7c3aed;height:6px;border-radius:4px;transition:width .4s}
  .setup{background:#1e1e2e22;border:1px dashed #334155;border-radius:10px;padding:32px;text-align:center;color:#64748b}
  .setup h3{color:#94a3b8;margin-bottom:8px}
  .setup code{background:#0a0a0f;padding:4px 10px;border-radius:4px;font-family:monospace;font-size:13px;color:#7c3aed}
  .sign-out{font-size:13px;color:#64748b;cursor:pointer;text-decoration:underline}
  .sign-out:hover{color:#94a3b8}
  #loading{text-align:center;padding:80px;color:#475569}
  #content{display:none}
  .refresh-btn{background:#1e1e2e;border:1px solid #334155;color:#94a3b8;border-radius:6px;padding:6px 14px;font-size:13px;cursor:pointer;float:right;margin-top:-4px}
  .refresh-btn:hover{background:#334155}
  .live-badge{display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#64748b;float:right;margin-top:0;margin-right:10px}
  .live-dot{width:7px;height:7px;border-radius:50%;background:#22c55e;animation:pulse 2s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
</style></head>
<body>
<nav class="nav">
  <div class="logo">⧠ Helix</div>
  <div style="display:flex;align-items:center;gap:12px">
    <span class="tenant-badge" id="tenant-label">loading...</span>
    <span class="sign-out" onclick="signOut()">Sign out</span>
  </div>
</nav>
<main>
  <div id="loading">⧠ Loading your dashboard...</div>
  <div id="content">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px">
      <h2>Overview <span id="period-label" style="font-size:14px;font-weight:400;color:#64748b"></span></h2>
      <span class="live-badge" id="live-badge" style="display:none"><span class="live-dot"></span><span id="last-update"></span></span>
      <button class="refresh-btn" onclick="load()">Refresh</button>
    </div>
    <div class="grid" id="stats-grid"></div>
    <div class="section">
      <h2 style="margin-bottom:16px">Daily Usage <span style="font-size:13px;font-weight:400;color:#64748b">(last 14 days)</span></h2>
      <div id="history-table"></div>
    </div>
    <div class="section" id="setup-section" style="display:none">
      <div class="setup">
        <h3>Connect your first machine</h3>
        <p style="margin-bottom:16px">Download the Helix node agent and add it to Claude Desktop to start capturing sessions.</p>
        <p><a href="/api/v1/init/agent" style="color:#7c3aed">Download agent.py</a></p>
        <p style="margin-top:12px;font-size:13px">Then run: <code>POST /api/v1/init</code> with your API key to get your Desktop config snippet.</p>
      </div>
    </div>
  </div>
</main>
<script>
  const TOKEN = localStorage.getItem('helix_token') || new URLSearchParams(window.location.search).get('key') || ''
  if(!TOKEN){ window.location.href='/login'; }

  function authHeaders(){
    if(TOKEN.startsWith('hx-')) return {'X-Helix-API-Key': TOKEN}
    return {'Authorization': 'Bearer ' + TOKEN}
  }

  function signOut(){ localStorage.removeItem('helix_token'); localStorage.removeItem('helix_tenant'); window.location.href='/login'; }

  function fmt(n){ return n>=1e6?(n/1e6).toFixed(1)+'M':n>=1e3?(n/1e3).toFixed(1)+'k':String(n) }

  async function load(){
    document.getElementById('loading').style.display='block'
    document.getElementById('content').style.display='none'
    try{
      const [u, h] = await Promise.all([
        fetch('/api/v1/usage?days=30', {headers:authHeaders()}).then(r=>r.json()),
        fetch('/api/v1/usage/history?days=14', {headers:authHeaders()}).then(r=>r.json()),
      ])
      if(u.detail && (u.detail.includes('401') || u.detail.includes('token'))){ window.location.href='/login'; return; }
      render(u, h)
    } catch(e){
      document.getElementById('loading').textContent = 'Error loading data. Check your key and try again.'
    }
  }

  function render(u, history){
    // Nav badge
    const badge = document.getElementById('tenant-label')
    badge.innerHTML = u.tenant + '<span class="plan-badge">' + u.plan + '</span>'
    document.title = 'Helix — ' + u.tenant_name

    document.getElementById('period-label').textContent = '(last 30 days)'

    // Stats grid
    const stats = [
      {label:'Total Tokens', val:fmt(u.tokens_total), sub: fmt(u.tokens_in)+' in / '+fmt(u.tokens_out)+' out'},
      {label:'Sessions', val:fmt(u.sessions), sub:'conversations captured'},
      {label:'Exchanges', val:fmt(u.exchanges), sub:'turns processed'},
      {label:'Tool Calls', val:fmt(u.tool_calls), sub:'MCP actions logged'},
      {label:'Observer Events', val:fmt(u.observer_actions), sub:'total pipeline events'},
    ]
    document.getElementById('stats-grid').innerHTML = stats.map(s=>`
      <div class="stat">
        <div class="stat-label">${s.label}</div>
        <div class="stat-val accent">${s.val}</div>
        <div class="stat-sub">${s.sub}</div>
      </div>`).join('')

    // History table
    const hist = document.getElementById('history-table')
    if(!history.length){
      hist.innerHTML = '<div class="empty">No data yet for this period</div>'
      document.getElementById('setup-section').style.display = u.sessions===0 ? 'block' : 'none'
    } else {
      const maxTok = Math.max(...history.map(r=>r.tokens_total), 1)
      hist.innerHTML = `<table>
        <thead><tr><th>Date</th><th>Sessions</th><th>Exchanges</th><th>Tokens In</th><th>Tokens Out</th><th>Total</th></tr></thead>
        <tbody>${history.map(r=>`
          <tr>
            <td>${r.day}</td>
            <td>${r.sessions}</td>
            <td>${r.exchanges}</td>
            <td>${fmt(r.tokens_in)}</td>
            <td>${fmt(r.tokens_out)}</td>
            <td>${fmt(r.tokens_total)}
              <div class="bar-track"><div class="bar-fill" style="width:${Math.round(r.tokens_total/maxTok*100)}%"></div></div>
            </td>
          </tr>`).join('')}
        </tbody></table>`
    }

    document.getElementById('loading').style.display='none'
    document.getElementById('content').style.display='block'
    const b=document.getElementById('live-badge'); b.style.display='inline-flex'
    document.getElementById('last-update').textContent='Updated '+new Date().toLocaleTimeString()
  }

  load()

  // Auto-refresh every 30s
  let _poll = setInterval(load, 30000)
  document.addEventListener('visibilitychange', () => {
    if(document.hidden){ clearInterval(_poll) }
    else { _poll = setInterval(load, 30000) }
  })
</script></body></html>
"""


@dashboard_router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_login_redirect(request: Request):
    """Redirect to central login if no session cookie."""
    from services.auth_service import get_session
    session = get_session(request)
    if not session:
        return RedirectResponse(url="https://helix.millyweb.com/login", status_code=302)
    slug = session.get("slug", "")
    return RedirectResponse(
        url=f"https://{slug}.helix.millyweb.com/dashboard/home",
        status_code=302
    )


@dashboard_router.get("/dashboard/_legacy", response_class=HTMLResponse, include_in_schema=False)
def dashboard_login():
    # Redirect to proper login page
    return HTMLResponse(status_code=302, headers={"Location": "/login"})


@dashboard_router.get("/dashboard/home", response_class=HTMLResponse, include_in_schema=False)
def dashboard_home(key: str = Query(default="")):
    """Serves dashboard shell. JS reads auth from localStorage (JWT) or ?key= param."""
    return HTMLResponse(_DASH_HTML)
