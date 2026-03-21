"""
widget.py — Bookmarkable compression/usage widget

GET /widget  — standalone floating widget, reads JWT from localStorage
              Works as a pinned browser tab for non-extension users.
"""
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

widget_router = APIRouter()

_WIDGET_HTML = '''
<!DOCTYPE html><html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Helix Widget</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  html,body{height:100%}
  body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0a0a0f;color:#e2e8f0;
    display:flex;align-items:center;justify-content:center;min-height:100vh}
  .widget{background:#13131a;border:1px solid #1e1e2e;border-radius:14px;padding:20px 22px;width:320px;
    box-shadow:0 8px 32px #0008}
  .header{display:flex;align-items:center;justify-content:space-between;margin-bottom:16px}
  .logo{font-size:16px;font-weight:700;color:#7c3aed}
  .live{display:flex;align-items:center;gap:5px;font-size:11px;color:#64748b}
  .dot{width:6px;height:6px;border-radius:50%;background:#22c55e;animation:pulse 2s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
  .grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px}
  .stat{background:#0a0a0f;border-radius:8px;padding:12px 14px}
  .stat-label{font-size:10px;color:#475569;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px}
  .stat-val{font-size:22px;font-weight:700;color:#7c3aed}
  .stat-sub{font-size:10px;color:#334155;margin-top:2px}
  .bar-row{margin-bottom:10px}
  .bar-label{display:flex;justify-content:space-between;font-size:11px;color:#64748b;margin-bottom:4px}
  .bar-track{background:#1e1e2e;border-radius:3px;height:5px}
  .bar-fill{background:linear-gradient(90deg,#7c3aed,#a78bfa);height:5px;border-radius:3px;transition:width .5s}
  .footer{display:flex;justify-content:space-between;align-items:center;margin-top:12px;padding-top:12px;
    border-top:1px solid #1e1e2e}
  .tenant{font-size:11px;color:#475569}
  .plan{font-size:10px;background:#7c3aed22;color:#a78bfa;padding:2px 7px;border-radius:4px;text-transform:uppercase}
  .err{color:#64748b;text-align:center;padding:20px;font-size:13px}
  .setup{text-align:center;padding:24px 0}
  .setup p{font-size:13px;color:#64748b;margin-bottom:12px}
  .setup a{color:#7c3aed;font-size:13px}
  #loading{color:#475569;font-size:13px;text-align:center;padding:20px}
</style></head>
<body>
<div class="widget">
  <div class="header">
    <div class="logo">⧠ Helix</div>
    <div class="live" id="live-ind" style="display:none">
      <div class="dot"></div><span id="ts"></span>
    </div>
  </div>
  <div id="loading">Loading...</div>
  <div id="body" style="display:none">
    <div class="grid" id="stats"></div>
    <div id="bars"></div>
    <div class="footer">
      <span class="tenant" id="tenant-label"></span>
      <span class="plan" id="plan-label"></span>
    </div>
  </div>
</div>
<script>
  const TOKEN = localStorage.getItem("helix_token") || new URLSearchParams(location.search).get("key") || ""

  function authH(){
    if(!TOKEN) return {}
    return TOKEN.startsWith("hx-") ? {"X-Helix-API-Key":TOKEN} : {"Authorization":"Bearer "+TOKEN}
  }

  function fmt(n){ return n>=1e6?(n/1e6).toFixed(1)+"M":n>=1e3?(n/1e3).toFixed(1)+"k":String(n) }

  async function refresh(){
    if(!TOKEN){
      document.getElementById("loading").innerHTML = "<div class=\'setup\'><p>Sign in to see your stats</p><a href=\'/login\'>Sign in</a></div>"
      return
    }
    try{
      const r = await fetch("/api/v1/usage?days=1", {headers: authH()})
      const d = await r.json()
      if(!r.ok || d.detail){ document.getElementById("loading").innerHTML = "<div class=\'err\'>Session expired. <a href=\'/login\' style=\'color:#7c3aed\'>Sign in</a></div>"; return }

      document.getElementById("loading").style.display = "none"
      document.getElementById("body").style.display = "block"
      document.getElementById("live-ind").style.display = "flex"
      document.getElementById("ts").textContent = new Date().toLocaleTimeString()
      document.getElementById("tenant-label").textContent = d.tenant
      document.getElementById("plan-label").textContent = d.plan

      // Stats grid
      const stats = [
        {label:"Tokens Today", val:fmt(d.tokens_total), sub:fmt(d.tokens_in)+" in / "+fmt(d.tokens_out)+" out"},
        {label:"Sessions", val:fmt(d.sessions), sub:"today"},
        {label:"Exchanges", val:fmt(d.exchanges), sub:"turns processed"},
        {label:"Tool Calls", val:fmt(d.tool_calls), sub:"MCP actions"},
      ]
      document.getElementById("stats").innerHTML = stats.map(s=>`
        <div class="stat">
          <div class="stat-label">${s.label}</div>
          <div class="stat-val">${s.val}</div>
          <div class="stat-sub">${s.sub}</div>
        </div>`).join("")

      // Bars for monthly context
      const m = await fetch("/api/v1/usage?days=30", {headers: authH()}).then(r=>r.json())
      const maxT = Math.max(m.tokens_total, 1)
      document.getElementById("bars").innerHTML = `
        <div class="bar-row">
          <div class="bar-label"><span>30-day tokens</span><span>${fmt(m.tokens_total)}</span></div>
          <div class="bar-track"><div class="bar-fill" style="width:${Math.min(100, Math.round(d.tokens_total/maxT*100*10))}%"></div></div>
        </div>`
    } catch(e){
      document.getElementById("loading").textContent = "Error loading data"
    }
  }

  refresh()
  setInterval(refresh, 30000)
  document.addEventListener("visibilitychange", () => { if(!document.hidden) refresh() })
</script></body></html>
'''

@widget_router.get("/widget", response_class=HTMLResponse, include_in_schema=False)
def widget_page():
    return HTMLResponse(_WIDGET_HTML)
