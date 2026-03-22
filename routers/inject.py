"""Inject Router — Full runbook injection for MCP pipeline

Pulls from:
1. Working KB docs (daily-status, handoff, operating-procedures)
2. Cortex SQLite (atoms, molecules, compression, pipeline)
3. Memory master context summary (LLM-compressed session history)

Budget: ~600 tokens (~2400 chars), priority-ordered sections.
"""
from fastapi import APIRouter, Query
from typing import Optional
from datetime import datetime, timezone, timedelta
from pathlib import Path
import json
from services import pg_sync
import re
import urllib.request
import urllib.error

router = APIRouter(prefix="/api/v1/inject", tags=["Inject - Context Pipeline"])

WORKING_KB = Path("/app/working-kb")
MEMORY_URL = "http://memory:9040"
MEMORY_TIMEOUT = 3


def _get_conn():
    from services.database import get_db
    db = get_db()
    return db.get_connection()


def _read_kb(relpath: str) -> str:
    try:
        p = WORKING_KB / relpath
        if p.exists():
            return p.read_text(encoding="utf-8")
    except Exception:
        pass
    return ""


def _fetch_memory_summary(max_chars=400) -> str:
    """Fetch Memory master context, extract project status lines."""
    try:
        req = urllib.request.Request(f"{MEMORY_URL}/api/summary", method="GET")
        with urllib.request.urlopen(req, timeout=MEMORY_TIMEOUT) as resp:
            data = pg_sync.dejson(resp.read().decode())
        raw = data.get("summary", "")
        if not raw:
            return ""
        projects = []
        lines = raw.splitlines()
        i = 0
        while i < len(lines):
            line = lines[i]
            if line.startswith("### "):
                name = line.replace("### ", "").strip()
                status = ""
                for j in range(i+1, min(i+4, len(lines))):
                    if lines[j].startswith("**Status:**"):
                        status = lines[j].replace("**Status:**", "").strip()
                        break
                if status:
                    projects.append(f"{name}: {status}")
                else:
                    projects.append(name)
            i += 1
        if not projects:
            for line in lines:
                stripped = line.strip()
                if stripped and not stripped.startswith("#") and not stripped.startswith("*") and not stripped.startswith("---"):
                    return stripped[:max_chars]
            return ""
        result = " | ".join(projects)
        if len(result) > max_chars:
            result = result[:max_chars - 3] + "..."
        return result
    except Exception as e:
        return f"(Memory unavailable: {type(e).__name__})"


def _extract_alerts(daily: str) -> list[str]:
    alerts = []
    in_alerts = False
    for line in daily.splitlines():
        if "INFRASTRUCTURE ALERTS" in line:
            in_alerts = True
            continue
        if in_alerts:
            if line.startswith("---") and "|" not in line:
                break
            if line.startswith("|---") or line.startswith("| Issue"):
                continue
            if line.startswith("|"):
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if len(parts) >= 2:
                    alerts.append(f"{parts[0]} [{parts[1]}]")
    return alerts


def _extract_projects(daily: str) -> list[str]:
    projects = []
    lines = daily.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("### ") and ("ACTIVE BUILDS" not in line):
            name = re.sub(r"^###\s*", "", line).strip()
            state = ""
            for j in range(i+1, min(i+6, len(lines))):
                if lines[j].startswith("**Current state:**"):
                    state = lines[j].replace("**Current state:**", "").strip()
                    if len(state) > 120:
                        state = state[:117] + "..."
                    break
                elif lines[j].startswith("**Last touched:**"):
                    m = re.search(r"Health:\s*(\S+)", lines[j])
                    if m and not state:
                        state = m.group(1)
            if state:
                projects.append(f"{name}: {state}")
            else:
                projects.append(name)
    return projects


def _extract_waiting(daily: str) -> list[str]:
    items = []
    in_waiting = False
    for line in daily.splitlines():
        if "WAITING ON RYAN" in line:
            in_waiting = True
            continue
        if in_waiting:
            if line.startswith("---") and "[" not in line:
                break
            if line.startswith("- ["):
                text = re.sub(r"^- \[.\]\s*", "", line).strip()
                if len(text) > 100:
                    text = text[:97] + "..."
                items.append(text)
    return items


def _extract_session_state(handoff: str) -> dict:
    """Parse the SESSION_STATE gate block from top of handoff.md."""
    state = {}
    in_gate = False
    for line in handoff.splitlines():
        if line.strip() == '---' and not in_gate:
            in_gate = True
            continue
        if line.strip() == '---' and in_gate:
            break
        if in_gate and ':' in line:
            k, _, v = line.partition(':')
            state[k.strip()] = v.strip()
    return state


def _extract_handoff_summary(handoff: str) -> str:
    # Check for session state gate first
    state = _extract_session_state(handoff)
    if state.get('NEXT'):
        prefix = f"[{state.get('SESSION_STATE','active').upper()}] Next: {state['NEXT']} | "
    else:
        prefix = ''
    lines = handoff.splitlines()
    summary_parts = []
    capturing = False
    for line in lines:
        if "Current State" in line or "COMPLETED" in line or "Phase Status" in line:
            capturing = True
            continue
        if capturing:
            if line.startswith("##") and summary_parts:
                break
            stripped = line.strip()
            if stripped and not stripped.startswith("|") and not stripped.startswith("---"):
                summary_parts.append(stripped)
                if len(" ".join(summary_parts)) > 200:
                    break
    text = prefix + " ".join(summary_parts)
    if len(text) > 250:
        text = text[:247] + "..."
    return text


def _build_cortex_stats() -> str:
    try:
        with _get_conn() as conn:
            atoms = conn.execute("SELECT COUNT(*) FROM atoms").fetchone()[0]
            molecules = conn.execute("SELECT COUNT(*) FROM molecules").fetchone()[0]
            organisms = conn.execute("SELECT COUNT(*) FROM organisms").fetchone()[0]
            ns = conn.execute("SELECT COUNT(*) FROM meta_namespaces").fetchone()[0]
            sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
            projects = conn.execute("""
                SELECT json_extract(meta, '$.provenance.first_seen_project') as project, COUNT(*) as cnt
                FROM atoms GROUP BY project ORDER BY cnt DESC LIMIT 5
            """).fetchall()
            proj_str = ", ".join(f"{p[0] or 'unknown'}({p[1]})" for p in projects)
            comp = conn.execute("""
                SELECT COUNT(*), COALESCE(SUM(tokens_original_in), 0), COALESCE(SUM(tokens_compressed_in), 0)
                FROM compression_log
            """).fetchone()
            comp_str = ""
            if comp and comp[0] > 0 and comp[1] > 0:
                saved = comp[1] - comp[2]
                ratio = round(comp[2] / comp[1] * 100, 1)
                comp_str = f" | Compression: {saved} tokens saved ({ratio}%)"
            return f"Cortex: {atoms} atoms, {molecules} mol, {organisms} org, {sessions} sessions | {ns} namespaces | Projects: {proj_str}{comp_str}"
    except Exception as e:
        return f"Cortex: error ({e})"


def _build_runbook(include_sections=None, max_chars=2400) -> dict:
    sections = include_sections or ["alerts", "projects", "handoff", "memory", "cortex", "waiting"]
    lines = []
    daily = _read_kb("cockpit/daily-status.md")
    handoff = _read_kb("projects/memory/handoff.md")
    if "alerts" in sections and daily:
        alerts = _extract_alerts(daily)
        if alerts:
            lines.append("ALERTS: " + " | ".join(alerts[:5]))
    if "projects" in sections and daily:
        projects = _extract_projects(daily)
        if projects:
            lines.append("ACTIVE: " + " // ".join(projects[:6]))
    if "handoff" in sections and handoff:
        summary = _extract_handoff_summary(handoff)
        if summary:
            lines.append("HANDOFF: " + summary)
    if "memory" in sections:
        mem = _fetch_memory_summary(max_chars=400)
        if mem:
            lines.append("MEMORY: " + mem)
    if "cortex" in sections:
        lines.append(_build_cortex_stats())
    if "waiting" in sections and daily:
        waiting = _extract_waiting(daily)
        if waiting:
            lines.append("WAITING ON RYAN: " + " | ".join(waiting[:4]))
    if "ops" in sections:
        lines.append("OPS: workspace_write for all files, checkpoint every ~15 calls, save to Working KB at session end")
    text = "\n".join(lines)
    if len(text) > max_chars:
        text = text[:max_chars - 3] + "..."
    return {
        "runbook": text,
        "chars": len(text),
        "estimated_tokens": len(text) // 4,
        "sections": sections,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": {"daily_status": bool(daily), "handoff": bool(handoff), "memory_api": True, "cortex_db": True}
    }


@router.get("/runbook")
async def get_runbook(
    sections: Optional[str] = Query(None, description="Comma-separated: alerts,projects,handoff,memory,cortex,waiting,ops"),
    max_chars: int = Query(2400, ge=200, le=5000, description="Hard character cap"),
    format: str = Query("text", description="text or json"),
):
    section_list = sections.split(",") if sections else None
    result = _build_runbook(include_sections=section_list, max_chars=max_chars)
    if format == "text":
        return {"text": result["runbook"], "chars": result["chars"], "tokens": result["estimated_tokens"]}
    return result


@router.get("/runbook/preview")
async def preview_runbook(sections: Optional[str] = Query(None)):
    section_list = sections.split(",") if sections else None
    result = _build_runbook(include_sections=section_list)
    full = f"--- HELIX CORTEX RUNBOOK ---\n{result['runbook']}\n--- END RUNBOOK ---"
    return {**result, "full_payload": full, "full_payload_chars": len(full), "full_payload_tokens": len(full) // 4}


@router.get("/health")
async def inject_health():
    try:
        result = _build_runbook(max_chars=500)
        kb_files = ["cockpit/daily-status.md", "projects/memory/handoff.md", "reference/operating-procedures.md"]
        kb_status = {f: (WORKING_KB / f).exists() for f in kb_files}
        mem_ok = False
        try:
            req = urllib.request.Request(f"{MEMORY_URL}/api/health", method="GET")
            with urllib.request.urlopen(req, timeout=2) as resp:
                mem_ok = resp.status == 200
        except Exception:
            pass
        return {
            "status": "healthy",
            "runbook_chars": result["chars"],
            "runbook_tokens": result["estimated_tokens"],
            "sections_available": ["alerts", "projects", "handoff", "memory", "cortex", "waiting", "ops"],
            "kb_sources": kb_status,
            "memory_api": mem_ok,
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}
