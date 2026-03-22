
"""File Events - subscribers that fire on every helix_file_write

Subscribers fire concurrently via asyncio.gather.
Each is independent - failure of one never blocks others.

Subscribers:
  git_sync     - commit + push to GitHub
  scan         - AST atom extraction (extract_all_from_source)
  kb_index     - knowledge base indexing (via Helix /api/v1/kb/index-file)
  kg_extract   - KG chain (build_kg_chain via intelligence_chain)
  forge_version - file versioning via Forge /api/workspace/write
  shard_diff   - context sharding
  sync_vps2    - sync helixmaster HTML to VPS2
"""
import asyncio
import logging
import subprocess
from pathlib import Path
from typing import Optional

log = logging.getLogger("helix.file_events")

FORGE_BASE = "http://10.0.8.6:9095"
HELIX_BASE = "http://127.0.0.1:9050"


def _sync_to_vps2(path: str) -> dict:
    try:
        result = subprocess.run(
            ["scp", path, f"root@10.0.0.2:{path}"],
            capture_output=True, timeout=30
        )
        if result.returncode == 0:
            log.info(f"[file_events] synced {path} to VPS2")
            return {"status": "synced", "path": path}
        else:
            return {"status": "error", "stderr": result.stderr.decode()[:200]}
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def dispatch_file_written(
    path: str,
    content: str,
    session_id: Optional[str] = None,
    steps: Optional[dict] = None,
) -> dict:
    """Run all file.written subscribers concurrently."""
    steps = steps or {}
    session_id = session_id or "helix"
    tasks = []
    labels = []

    do_git      = steps.get("git", True)
    do_scan     = steps.get("scan", True)
    do_kb       = steps.get("kb", True)
    do_kg       = steps.get("kg", True)
    do_forge    = steps.get("forge", True)
    do_shard    = steps.get("shard", True)
    do_snapshot = steps.get("snapshot", False)

    # ── git_sync ────────────────────────────────────────────────
    if do_git:
        try:
            from services.git_sync import auto_commit
            tasks.append(asyncio.to_thread(auto_commit, path, session_id, "file.written"))
            labels.append("git_sync")
        except Exception as e:
            log.debug(f"git_sync unavailable: {e}")

    # ── scan (AST atom extraction) ───────────────────────────────
    if do_scan and path.endswith((".py", ".js", ".ts", ".jsx", ".tsx")):
        try:
            from services.scanner import extract_all_from_source
            tasks.append(asyncio.to_thread(extract_all_from_source, content, path))
            labels.append("scan")
        except Exception as e:
            log.debug(f"scan unavailable: {e}")

    # ── kb_index (via Helix REST) ────────────────────────────────
    if do_kb:
        try:
            import httpx
            src = (
                "working-kb" if "/working-kb/" in path
                else "infra-kb" if "/infra-kb/" in path
                else "workbench"
            )
            async def _kb_index(p=path, c=content, s=src, sid=session_id):
                async with httpx.AsyncClient(timeout=15) as client:
                    r = await client.post(
                        f"{HELIX_BASE}/api/v1/kb/index-file",
                        json={"path": p, "content": c, "source": s, "session_id": sid}
                    )
                    return r.json() if r.status_code == 200 else {"status": "error", "code": r.status_code}
            tasks.append(_kb_index())
            labels.append("kb_index")
        except Exception as e:
            log.debug(f"kb_index unavailable: {e}")

    # ── kg_extract (intelligence_chain) ─────────────────────────
    if do_kg:
        try:
            from services.intelligence_chain import build_kg_chain
            from services import pg_sync
            def _kg_extract(p=path, c=content, sid=session_id):
                conn = pg_sync.sqlite_conn()
                try:
                    # Use path as a lightweight decision proxy for KG
                    result = build_kg_chain(f"file written: {p}", sid, conn)
                    return result
                finally:
                    conn.close()
            tasks.append(asyncio.to_thread(_kg_extract))
            labels.append("kg_extract")
        except Exception as e:
            log.debug(f"kg_extract unavailable: {e}")

    # ── forge_version (Forge /api/workspace/write) ───────────────
    if do_forge and content:
        try:
            import httpx
            async def _forge_version(p=path, c=content, sid=session_id):
                async with httpx.AsyncClient(timeout=15) as client:
                    r = await client.post(
                        f"{FORGE_BASE}/api/workspace/write",
                        json={"path": p, "content": c, "session_id": sid}
                    )
                    return r.json() if r.status_code == 200 else {"status": "error", "code": r.status_code}
            tasks.append(_forge_version())
            labels.append("forge_version")
        except Exception as e:
            log.debug(f"forge_version unavailable: {e}")

    # ── shard_diff ───────────────────────────────────────────────
    if do_shard:
        try:
            from services.shard import get_shard_service
            svc = get_shard_service()
            tasks.append(asyncio.to_thread(svc.diff_on_write, path, content, session_id))
            labels.append("shard_differ")
        except Exception as e:
            log.debug(f"shard_differ unavailable: {e}")

    # ── snapshot ─────────────────────────────────────────────────
    if do_snapshot:
        try:
            from services.snapshots import queue_snapshot
            tasks.append(queue_snapshot(path, session_id))
            labels.append("snapshot")
        except Exception as e:
            log.debug(f"snapshot unavailable: {e}")

    # ── helixmaster VPS2 sync ────────────────────────────────────
    if "helixmaster" in path and path.endswith(".html"):
        tasks.append(asyncio.to_thread(_sync_to_vps2, path))
        labels.append("sync_vps2")

    if not tasks:
        return {"event": "file.written", "path": path, "subscribers": 0, "results": {}}

    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results = {}
    for label, result in zip(labels, raw_results):
        if isinstance(result, Exception):
            log.warning(f"[file_events] {label} failed: {result}")
            results[label] = {"status": "error", "error": str(result)}
        else:
            results[label] = result or {"status": "ok"}

    log.info(f"[file_events] {path}: {list(results.keys())}")
    return {
        "event": "file.written",
        "path": path,
        "subscribers": len(tasks),
        "results": results,
    }


async def handle_file_written(payload: dict) -> dict:
    """Event router entry point — unpacks payload and dispatches.

    Normalises the step flag names from workbench format
    (version/index/entities) to subscriber format (forge/kb/kg).
    """
    path       = payload.get("path", "")
    content    = payload.get("content", "")
    session_id = payload.get("session_id", "helix")
    raw        = payload.get("steps", {})

    steps = {
        "git":      raw.get("git",   True),
        "scan":     raw.get("scan",  True),
        "kb":       raw.get("index", raw.get("kb",    True)),
        "kg":       raw.get("entities", raw.get("kg", True)),
        "forge":    raw.get("version", raw.get("forge", True)),
        "shard":    raw.get("shard", True),
        "snapshot": raw.get("snapshot", False),
    }

    return await dispatch_file_written(path, content, session_id, steps)


async def handle_file_read(payload: dict) -> dict:
    """Stub for file.read events — currently a no-op."""
    return {"event": "file.read", "path": payload.get("path", ""), "status": "ok"}
