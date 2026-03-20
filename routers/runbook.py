"""Runbook Router - Dynamic registry of contextual docs with slim index injection

The runbook is a registry of pages - each page is a fetchable document with:
- source: where the content comes from (KB file, API endpoint, cortex DB, static text)
- triggers: when Claude should read it (before VPS work, MCP install, on demand)
- category: organizational grouping (project, operations, infrastructure, reference)

The auto-inject only sends a slim INDEX (~100-150 tokens) listing available pages.
Full page content is fetched on-demand when the task matches a trigger.

CRUD API lets Ryan (or Claude) add/update/remove pages conversationally.
"""
from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, timezone
import json
from services import pg_sync
import urllib.request
import urllib.error
from pathlib import Path

router = APIRouter(prefix="/api/v1/runbook", tags=["Runbook - Dynamic Context Registry"])

WORKING_KB = Path("/app/working-kb")
MEMORY_URL = "http://memory:9040"


def _get_conn():
    from services.database import get_db
    db = get_db()
    return db.get_connection()


def _ensure_table():
    """No-op: tables exist in PostgreSQL (migration 001)."""
    pass
class PageCreate(BaseModel):
    id: str = Field(..., description="Unique slug")
    name: str = Field(..., description="Display name")
    description: str = Field("", description="What this page contains")
    category: str = Field("reference", description="project | operations | infrastructure | reference")
    source_type: str = Field("static", description="kb_file | endpoint | memory_api | cortex_query | static")
    source_config: dict = Field(default_factory=dict)
    triggers: list[str] = Field(default_factory=list)
    priority: int = Field(50)
    content: Optional[str] = Field(None)


class PageUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    source_type: Optional[str] = None
    source_config: Optional[dict] = None
    triggers: Optional[list[str]] = None
    priority: Optional[int] = None
    active: Optional[bool] = None
    content: Optional[str] = None


def _fetch_page_content(page: dict) -> str:
    source_type = page["source_type"]
    config = pg_sync.dejson(page["source_config"]) if isinstance(page["source_config"], str) else page["source_config"]
    if source_type == "static":
        return config.get("content", "(no content)")
    elif source_type == "kb_file":
        path = config.get("path", "")
        if not path:
            return "(no KB path configured)"
        try:
            p = WORKING_KB / path
            if p.exists():
                text = p.read_text(encoding="utf-8")
                max_chars = config.get("max_chars", 5000)
                if len(text) > max_chars:
                    text = text[:max_chars] + "\n... (truncated)"
                return text
            return f"(KB file not found: {path})"
        except Exception as e:
            return f"(KB read error: {e})"
    elif source_type == "endpoint":
        url = config.get("url", "")
        if not url:
            return "(no endpoint URL configured)"
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=config.get("timeout", 5)) as resp:
                data = resp.read().decode()
            try:
                parsed = pg_sync.dejson(data)
                for key in ["text", "summary", "content", "runbook", "result"]:
                    if key in parsed and isinstance(parsed[key], str):
                        return parsed[key]
                return json.dumps(parsed, indent=2)
            except json.JSONDecodeError:
                return data
        except Exception as e:
            return f"(endpoint error: {e})"
    elif source_type == "memory_api":
        try:
            endpoint = config.get("endpoint", "/api/summary")
            req = urllib.request.Request(f"{MEMORY_URL}{endpoint}", method="GET")
            with urllib.request.urlopen(req, timeout=3) as resp:
                data = pg_sync.dejson(resp.read().decode())
            return data.get(config.get("response_key", "summary"), str(data))
        except Exception as e:
            return f"(Memory API error: {e})"
    elif source_type == "cortex_query":
        try:
            query = config.get("query", "")
            if not query:
                return "(no query configured)"
            with _get_conn() as conn:
                rows = conn.execute(query).fetchall()
                return "\n".join(" | ".join(str(v) for v in row) for row in rows[:50]) or "(no results)"
        except Exception as e:
            return f"(cortex query error: {e})"
    return f"(unknown source_type: {source_type})"


@router.post("/pages")
async def create_page(page: PageCreate):
    _ensure_table()
    now = datetime.now(timezone.utc).isoformat()
    source_config = page.source_config.copy()
    if page.source_type == "static" and page.content:
        source_config["content"] = page.content
    with _get_conn() as conn:
        if conn.execute("SELECT id FROM runbook_pages WHERE id = ?", (page.id,)).fetchone():
            raise HTTPException(status_code=409, detail=f"Page '{page.id}' exists. Use PUT to update.")
        conn.execute("INSERT INTO runbook_pages VALUES (?,?,?,?,?,?,?,?,1,?,?)",
            (page.id, page.name, page.description, page.category, page.source_type,
             json.dumps(source_config), json.dumps(page.triggers), page.priority, now, now))
        conn.commit()
    return {"status": "created", "id": page.id, "name": page.name}


@router.put("/pages/{page_id}")
async def update_page(page_id: str, update: PageUpdate):
    _ensure_table()
    now = datetime.now(timezone.utc).isoformat()
    with _get_conn() as conn:
        existing = conn.execute("SELECT * FROM runbook_pages WHERE id = ?", (page_id,)).fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail=f"Page '{page_id}' not found")
        cols = ["id","name","description","category","source_type","source_config","triggers","priority","active","created_at","updated_at"]
        current = dict(zip(cols, existing))
        fields = {}
        if update.name is not None: fields["name"] = update.name
        if update.description is not None: fields["description"] = update.description
        if update.category is not None: fields["category"] = update.category
        if update.source_type is not None: fields["source_type"] = update.source_type
        if update.source_config is not None:
            config = update.source_config.copy()
            if update.content: config["content"] = update.content
            fields["source_config"] = json.dumps(config)
        elif update.content is not None:
            config = pg_sync.dejson(current["source_config"])
            config["content"] = update.content
            fields["source_config"] = json.dumps(config)
        if update.triggers is not None: fields["triggers"] = json.dumps(update.triggers)
        if update.priority is not None: fields["priority"] = update.priority
        if update.active is not None: fields["active"] = 1 if update.active else 0
        fields["updated_at"] = now
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        conn.execute(f"UPDATE runbook_pages SET {set_clause} WHERE id = ?", list(fields.values()) + [page_id])
        conn.commit()
    return {"status": "updated", "id": page_id, "fields_changed": list(fields.keys())}


@router.delete("/pages/{page_id}")
async def delete_page(page_id: str):
    _ensure_table()
    with _get_conn() as conn:
        if not conn.execute("SELECT id FROM runbook_pages WHERE id = ?", (page_id,)).fetchone():
            raise HTTPException(status_code=404, detail=f"Page '{page_id}' not found")
        conn.execute("DELETE FROM runbook_pages WHERE id = ?", (page_id,))
        conn.commit()
    return {"status": "deleted", "id": page_id}


@router.get("/pages")
async def list_pages(category: Optional[str] = None, active_only: bool = True, trigger: Optional[str] = None):
    _ensure_table()
    query = "SELECT * FROM runbook_pages"
    conditions, params = [], []
    if active_only: conditions.append("active = 1")
    if category: conditions.append("category = ?"); params.append(category)
    if conditions: query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY priority ASC, name ASC"
    with _get_conn() as conn:
        cols = ["id","name","description","category","source_type","source_config","triggers","priority","active","created_at","updated_at"]
        rows = conn.execute(query, params).fetchall()
    pages = []
    for row in rows:
        page = dict(zip(cols, row))
        page["source_config"] = pg_sync.dejson(page["source_config"])
        page["triggers"] = pg_sync.dejson(page["triggers"])
        page["active"] = bool(page["active"])
        if trigger and trigger not in page["triggers"]: continue
        if "content" in page["source_config"]:
            page["source_config"]["content"] = f"({len(page['source_config']['content'])} chars)"
        pages.append(page)
    return {"pages": pages, "count": len(pages)}


@router.get("/pages/{page_id}")
async def get_page(page_id: str, fetch_content: bool = True):
    _ensure_table()
    with _get_conn() as conn:
        cols = ["id","name","description","category","source_type","source_config","triggers","priority","active","created_at","updated_at"]
        row = conn.execute("SELECT * FROM runbook_pages WHERE id = ?", (page_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Page '{page_id}' not found")
    page = dict(zip(cols, row))
    page["source_config"] = pg_sync.dejson(page["source_config"])
    page["triggers"] = pg_sync.dejson(page["triggers"])
    page["active"] = bool(page["active"])
    if fetch_content:
        page["content"] = _fetch_page_content(page)
        page["content_chars"] = len(page["content"])
        page["content_tokens"] = len(page["content"]) // 4
    return page


@router.get("/index")
async def get_index(format: str = Query("text", description="text or json")):
    """Generate slim index for auto-injection (~100-150 tokens)."""
    _ensure_table()
    alerts_summary = ""
    try:
        daily = (WORKING_KB / "cockpit/daily-status.md").read_text(encoding="utf-8")
        from routers.inject import _extract_alerts
        alerts = _extract_alerts(daily)
        if alerts:
            c = sum(1 for a in alerts if "Critical" in a)
            m = sum(1 for a in alerts if "Medium" in a)
            parts = []
            if c: parts.append(f"{c} critical")
            if m: parts.append(f"{m} medium")
            alerts_summary = ", ".join(parts)
    except Exception:
        alerts_summary = "unknown"
    handoff_line = ""
    try:
        handoff = (WORKING_KB / "projects/memory/handoff.md").read_text(encoding="utf-8")
        from routers.inject import _extract_handoff_summary
        handoff_line = _extract_handoff_summary(handoff)
        if len(handoff_line) > 120: handoff_line = handoff_line[:117] + "..."
    except Exception:
        handoff_line = "unavailable"
    with _get_conn() as conn:
        rows = conn.execute("SELECT id, name, description, category, triggers FROM runbook_pages WHERE active = 1 ORDER BY priority ASC").fetchall()
    categories = {}
    trigger_rules = []
    for page_id, name, desc, category, triggers_json in rows:
        triggers = pg_sync.dejson(triggers_json)
        cat_list = categories.setdefault(category, [])
        cat_list.append(name)
        for t in triggers:
            if t != "on_demand":
                trigger_rules.append(f"{t}->'{page_id}'")
    lines = ["--- HELIX ---"]
    if alerts_summary: lines.append(f"ALERTS: {alerts_summary}")
    if handoff_line: lines.append(f"RESUME: {handoff_line}")
    if categories:
        lines.append("PAGES:")
        for cat, items in categories.items():
            lines.append(f"  [{cat}] {' | '.join(items)}")
    if trigger_rules:
        seen = set()
        unique = [t for t in trigger_rules if t not in seen and not seen.add(t)]
        lines.append("AUTO: " + " | ".join(unique[:10]))
    lines.append("FETCH: ssh_execute curl -s http://127.0.0.1:9050/api/v1/runbook/pages/{id}")
    lines.append("--- END ---")
    text = "\n".join(lines)
    if format == "json":
        return {"index": text, "chars": len(text), "tokens": len(text) // 4,
                "page_count": sum(len(v) for v in categories.values()),
                "generated_at": datetime.now(timezone.utc).isoformat()}
    return {"text": text, "chars": len(text), "tokens": len(text) // 4}


@router.post("/seed")
async def seed_defaults():
    _ensure_table()
    now = datetime.now(timezone.utc).isoformat()
    defaults = [
        {"id": "master-summary", "name": "Master Summary", "description": "Full compressed project history from Memory", "category": "project",
         "source_type": "memory_api", "source_config": {"endpoint": "/api/summary", "response_key": "summary"},
         "triggers": ["project_context", "session_start", "on_demand"], "priority": 10},
        {"id": "active-projects", "name": "Active Projects", "description": "Current project statuses", "category": "project",
         "source_type": "kb_file", "source_config": {"path": "cockpit/daily-status.md", "max_chars": 4000},
         "triggers": ["project_context", "on_demand"], "priority": 15},
        {"id": "operating-procedures", "name": "Operating Procedures", "description": "VPS rules, DB safety, checkpoints", "category": "operations",
         "source_type": "kb_file", "source_config": {"path": "reference/operating-procedures.md", "max_chars": 5000},
         "triggers": ["before_vps_work", "before_db_work", "session_start"], "priority": 20},
        {"id": "context-handoff", "name": "Context Handoff", "description": "Resume instructions from last session", "category": "operations",
         "source_type": "kb_file", "source_config": {"path": "projects/memory/handoff.md", "max_chars": 3000},
         "triggers": ["session_start", "on_demand"], "priority": 5},
        {"id": "alerts-detail", "name": "Alerts Detail", "description": "Full infra alerts with severity", "category": "infrastructure",
         "source_type": "kb_file", "source_config": {"path": "cockpit/daily-status.md", "max_chars": 2000},
         "triggers": ["infrastructure_issue", "on_demand"], "priority": 8},
        {"id": "mcp-install-guide", "name": "MCP Install Guide", "description": "Runbook for adding MCP servers", "category": "infrastructure",
         "source_type": "kb_file", "source_config": {"path": "mcp/install-mcp-server.md", "max_chars": 8000},
         "triggers": ["mcp_install", "mcp_server_work"], "priority": 40},
        {"id": "cortex-stats", "name": "Cortex Stats", "description": "Atoms, molecules, compression", "category": "infrastructure",
         "source_type": "endpoint", "source_config": {"url": "http://127.0.0.1:9050/api/v1/cockpit/dna"},
         "triggers": ["helix_work", "on_demand"], "priority": 45},
        {"id": "system-health", "name": "System Health", "description": "Full cockpit overview", "category": "infrastructure",
         "source_type": "endpoint", "source_config": {"url": "http://127.0.0.1:9050/api/v1/cockpit/overview"},
         "triggers": ["infrastructure_issue", "monitoring", "on_demand"], "priority": 35},
    ]
    created, skipped = [], []
    with _get_conn() as conn:
        for p in defaults:
            if conn.execute("SELECT id FROM runbook_pages WHERE id = ?", (p["id"],)).fetchone():
                skipped.append(p["id"]); continue
            conn.execute("INSERT INTO runbook_pages VALUES (?,?,?,?,?,?,?,?,1,?,?)",
                (p["id"], p["name"], p["description"], p["category"], p["source_type"],
                 json.dumps(p["source_config"]), json.dumps(p["triggers"]), p["priority"], now, now))
            created.append(p["id"])
        conn.commit()
    return {"created": created, "skipped": skipped, "total": len(created) + len(skipped)}
