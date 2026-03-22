"""
register.py — Self-serve registration + login pages

GET /register  — multi-step signup wizard
GET /login     — login page
"""
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

register_router = APIRouter()

_REGISTER_HTML = '''
<!DOCTYPE html><html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Helix — Get Started</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0a0a0f;color:#e2e8f0;min-height:100vh;display:flex;align-items:center;justify-content:center}
  .wrap{width:100%;max-width:480px;padding:24px}
  .card{background:#13131a;border:1px solid #1e1e2e;border-radius:12px;padding:40px}
  .logo{font-size:22px;font-weight:700;color:#7c3aed;margin-bottom:4px}
  .sub{color:#64748b;font-size:14px;margin-bottom:28px}
  .step{display:none}.step.active{display:block}
  .step-label{font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin-bottom:20px}
  label{display:block;font-size:13px;color:#94a3b8;margin-bottom:5px;margin-top:14px}
  input{width:100%;background:#0a0a0f;border:1px solid #1e1e2e;border-radius:8px;padding:11px 13px;color:#e2e8f0;font-size:14px;outline:none}
  input:focus{border-color:#7c3aed}
  .btn{width:100%;margin-top:20px;background:#7c3aed;border:none;border-radius:8px;padding:12px;color:#fff;font-size:15px;font-weight:600;cursor:pointer}
  .btn:hover{background:#6d28d9}
  .btn-sec{background:#1e1e2e;color:#94a3b8}
  .btn-sec:hover{background:#334155}
  .err{color:#f87171;font-size:13px;margin-top:10px;display:none}
  .opts{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:8px}
  .opt{background:#0a0a0f;border:2px solid #1e1e2e;border-radius:8px;padding:14px;cursor:pointer;text-align:center;font-size:13px;transition:border-color .15s}
  .opt:hover{border-color:#7c3aed55}
  .opt.sel{border-color:#7c3aed;background:#7c3aed11}
  .opt .icon{font-size:24px;margin-bottom:6px}
  .rec{background:#7c3aed11;border:1px solid #7c3aed33;border-radius:10px;padding:18px;margin-top:16px}
  .rec-title{font-size:15px;font-weight:600;color:#a78bfa;margin-bottom:6px}
  .rec-desc{font-size:13px;color:#94a3b8;line-height:1.5}
  .dl-btn{display:block;width:100%;margin-top:12px;background:#7c3aed;border:none;border-radius:8px;padding:11px;color:#fff;font-size:14px;font-weight:600;cursor:pointer;text-align:center;text-decoration:none}
  .dl-btn:hover{background:#6d28d9}
  .code-block{background:#0a0a0f;border:1px solid #1e1e2e;border-radius:6px;padding:12px;font-family:monospace;font-size:12px;color:#a78bfa;white-space:pre-wrap;margin-top:8px;word-break:break-all}
  .done{text-align:center;padding:20px 0}
  .done .check{font-size:48px;margin-bottom:12px}
  .done h3{font-size:20px;font-weight:600;margin-bottom:8px}
  .done p{color:#64748b;font-size:14px;margin-bottom:20px}
  .login-link{text-align:center;margin-top:16px;font-size:13px;color:#64748b}
  .login-link a{color:#7c3aed;text-decoration:none}
</style></head>
<body>
<div class="wrap">
<div class="card">
  <div class="logo">⧠ Helix</div>
  <div class="sub">AI memory and compression for everyone</div>

  <!-- Step 1: Account -->
  <div class="step active" id="s1">
    <div class="step-label">Step 1 of 3 — Create account</div>
    <label>Email</label><input id="email" type="email" placeholder="you@example.com" autocomplete="email">
    <label>Password</label><input id="pw" type="password" placeholder="8+ characters">
    <label>Your name (optional)</label><input id="name" placeholder="Ashley">
    <button class="btn" onclick="step1()">Continue</button>
    <div class="err" id="err1"></div>
    <div class="login-link">Already have an account? <a href="/login">Sign in</a></div>
  </div>

  <!-- Step 2: LLM questionnaire -->
  <div class="step" id="s2">
    <div class="step-label">Step 2 of 3 — Your setup</div>
    <div style="font-size:14px;color:#94a3b8;margin-bottom:14px">Which AI tools do you use?</div>
    <div class="opts">
      <div class="opt" id="llm-claude" onclick="pick('llm','claude',this)">
        <div class="icon">✨</div>Claude
      </div>
      <div class="opt" id="llm-chatgpt" onclick="pick('llm','chatgpt',this)">
        <div class="icon">🤖</div>ChatGPT
      </div>
      <div class="opt" id="llm-gemini" onclick="pick('llm','gemini',this)">
        <div class="icon">💎</div>Gemini
      </div>
      <div class="opt" id="llm-multi" onclick="pick('llm','multi',this)">
        <div class="icon">🔀</div>Multiple
      </div>
    </div>
    <div style="font-size:14px;color:#94a3b8;margin:18px 0 10px">What matters most?</div>
    <div class="opts">
      <div class="opt" id="goal-memory" onclick="pick('goal','memory',this)">
        <div class="icon">🧠</div>Remember everything
      </div>
      <div class="opt" id="goal-compress" onclick="pick('goal','compress',this)">
        <div class="icon">⚡</div>Reduce token costs
      </div>
      <div class="opt" id="goal-both" onclick="pick('goal','both',this)">
        <div class="icon">⭐</div>Both
      </div>
      <div class="opt" id="goal-explore" onclick="pick('goal','explore',this)">
        <div class="icon">🔭</div>Just exploring
      </div>
    </div>
    <button class="btn" onclick="step2()" style="margin-top:20px">See recommendation</button>
    <div class="err" id="err2"></div>
  </div>

  <!-- Step 3: Recommendation + install -->
  <div class="step" id="s3">
    <div class="step-label">Step 3 of 3 — Get set up</div>
    <div class="rec" id="rec-box"></div>
    <button class="btn btn-sec" onclick="step3done()" style="margin-top:16px">Skip for now → Go to dashboard</button>
  </div>

  <!-- Done -->
  <div class="step" id="s4">
    <div class="done">
      <div class="check">✅</div>
      <h3>You\'re all set!</h3>
      <p>Your Helix workspace is ready.</p>
      <a class="dl-btn" id="dash-link" href="/dashboard">Open Dashboard →</a>
    </div>
  </div>

</div></div>
<script>
  const S = {llm:null, goal:null, token:null, slug:null};

  function show(id){["s1","s2","s3","s4"].forEach(s=>document.getElementById(s).classList.remove("active"));document.getElementById(id).classList.add("active");}
  function err(id,msg){const el=document.getElementById(id);el.textContent=msg;el.style.display=msg?"block":"none";}
  function pick(group,val,el){document.querySelectorAll(`[id^="${group}-"]`).forEach(e=>e.classList.remove("sel"));el.classList.add("sel");S[group]=val;}

  document.getElementById("email").addEventListener("keydown",e=>e.key==="Enter"&&document.getElementById("pw").focus());
  document.getElementById("pw").addEventListener("keydown",e=>e.key==="Enter"&&step1());

  async function step1(){
    const email=document.getElementById("email").value.trim();
    const pw=document.getElementById("pw").value;
    const name=document.getElementById("name").value.trim();
    if(!email||!pw){err("err1","Email and password required.");return;}
    if(pw.length<8){err("err1","Password must be 8+ characters.");return;}
    err("err1","");
    try{
      const r=await fetch("/api/v1/auth/register",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({email,password:pw,name:name||undefined})});
      const d=await r.json();
      if(!r.ok){err("err1",d.detail||"Registration failed.");return;}
      S.token=d.token;S.slug=d.tenant;
      localStorage.setItem("helix_token",d.token);
      localStorage.setItem("helix_tenant",d.tenant);
      show("s2");
    }catch(e){err("err1","Network error. Try again.");}
  }

  function step2(){
    if(!S.llm){err("err2","Pick an AI tool first.");return;}
    if(!S.goal){err("err2","Pick your priority.");return;}
    err("err2","");
    const recs={
      "claude+memory":  {title:"MemBrain Extension (Paid)",desc:"Connects directly to Claude.ai. Remembers everything across sessions. Includes compression.",action:"chrome",link:"https://chromewebstore.google.com"},
      "claude+compress": {title:"MemBrain Extension (Free)",desc:"Reduces token costs by 30-60% on every Claude session. Works in your browser.",action:"chrome",link:"https://chromewebstore.google.com"},
      "claude+both":     {title:"MemBrain Extension (Paid)",desc:"Full memory + compression for Claude. Best value option.",action:"chrome",link:"https://chromewebstore.google.com"},
      "chatgpt+memory":  {title:"Helix Browser Extension",desc:"Memory layer for ChatGPT and Gemini. Cross-session context that follows you.",action:"chrome",link:"https://chromewebstore.google.com"},
      "gemini+memory":   {title:"Helix Browser Extension",desc:"Memory layer for Gemini. Works alongside Google\'s AI tools.",action:"chrome",link:"https://chromewebstore.google.com"},
      "multi+both":      {title:"Helix Node Agent + Extension",desc:"Works across all AI tools. MCP for Claude Desktop, extension for browsers.",action:"agent",link:"/api/v1/init/agent"},
    };
    const key=`${S.llm}+${S.goal}`;
    const rec=recs[key]||recs[`${S.llm}+both`]||{title:"Helix Dashboard",desc:"Start tracking your AI usage. Connect tools whenever you\'re ready.",action:"dash",link:"/dashboard"};
    let actionHtml="";
    if(rec.action==="chrome")actionHtml=`<a class="dl-btn" href="${rec.link}" target="_blank">📱 Get Chrome Extension</a>`;
    else if(rec.action==="agent")actionHtml=`<a class="dl-btn" href="${rec.link}">Download Node Agent (agent.py)</a>`;
    else actionHtml=`<a class="dl-btn" href="${rec.link}">Open Dashboard →</a>`;
    document.getElementById("rec-box").innerHTML=`<div class="rec-title">🔑 Recommended: ${rec.title}</div><div class="rec-desc">${rec.desc}</div>${actionHtml}`;
    show("s3");
  }

  function step3done(){
    window.location.href="/dashboard/home";
  }
</script></body></html>
'''

_LOGIN_HTML = '''
<!DOCTYPE html><html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Helix — Sign In</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0a0a0f;color:#e2e8f0;min-height:100vh;display:flex;align-items:center;justify-content:center}
  .card{background:#13131a;border:1px solid #1e1e2e;border-radius:12px;padding:40px;width:100%;max-width:400px}
  .logo{font-size:22px;font-weight:700;color:#7c3aed;margin-bottom:4px}
  .sub{color:#64748b;font-size:14px;margin-bottom:28px}
  label{display:block;font-size:13px;color:#94a3b8;margin-bottom:5px;margin-top:14px}
  input{width:100%;background:#0a0a0f;border:1px solid #1e1e2e;border-radius:8px;padding:11px 13px;color:#e2e8f0;font-size:14px;outline:none}
  input:focus{border-color:#7c3aed}
  .btn{width:100%;margin-top:20px;background:#7c3aed;border:none;border-radius:8px;padding:12px;color:#fff;font-size:15px;font-weight:600;cursor:pointer}
  .btn:hover{background:#6d28d9}
  .err{color:#f87171;font-size:13px;margin-top:10px;display:none}
  .foot{text-align:center;margin-top:16px;font-size:13px;color:#64748b}
  .foot a{color:#7c3aed;text-decoration:none}
</style></head>
<body>
<div class="card">
  <div class="logo">⧠ Helix</div>
  <div class="sub">Sign in to your workspace</div>
  <label>Email</label><input id="email" type="email" autocomplete="email">
  <label>Password</label><input id="pw" type="password">
  <button class="btn" onclick="doLogin()">Sign In</button>
  <div class="err" id="err"></div>
  <div class="foot">No account? <a href="/register">Get started free</a></div>
</div>
<script>
  document.getElementById("pw").addEventListener("keydown",e=>e.key==="Enter"&&doLogin());
  async function doLogin(){
    const email=document.getElementById("email").value.trim();
    const pw=document.getElementById("pw").value;
    const el=document.getElementById("err");
    el.style.display="none";
    try{
      const r=await fetch("/api/v1/auth/login",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({email,password:pw})});
      const d=await r.json();
      if(!r.ok){el.textContent=d.detail||"Invalid credentials.";el.style.display="block";return;}
      localStorage.setItem("helix_token",d.token);
      localStorage.setItem("helix_tenant",d.tenant_id);
      window.location.href="/dashboard/home";
    }catch(e){el.textContent="Network error.";el.style.display="block";}
  }
</script></body></html>
'''

@register_router.get("/register", response_class=HTMLResponse, include_in_schema=False)
def register_page():
    return HTMLResponse(_REGISTER_HTML)

@register_router.get("/login", response_class=HTMLResponse, include_in_schema=False)
def login_page():
    return HTMLResponse(_LOGIN_HTML)
