"""helix_action — Unified Action Router

Single entry point for all file operations and commands.
The model calls one thing. Helix decides what to do with it.

Action types:
  file_write    — write file + full pipeline (version, scan, KB, KG, observer)
  file_patch    — str_replace edit + full pipeline
  file_move     — move on disk + KG path label update
  file_delete   — delete + observer log
  file_read     — return content (no pipeline)
  file_list     — directory listing (no pipeline)
  command       — run shell command + observer log
  scaffold_query — query atom store for scaffold context

All write operations fire async Pipeline A (learn).
All write operations return Pipeline B scaffold alongside result.
"""
import logging
from typing import Optional, Any, Dict
from fastapi import APIRouter, HTTPException
from routers.node_router import route_file_write as _node_file_write, route_command as _node_command, route_file_read as _node_file_read
from pydantic import BaseModel

log = logging.getLogger("helix.action")

router = APIRouter(prefix="/api/v1")


# ================================================================
# REQUEST MODELS
# ================================================================

class ActionRequest(BaseModel):
    type: str
    # File ops
    path: Optional[str] = None
    content: Optional[str] = None
    # file_patch
    old_str: Optional[str] = None
    new_str: Optional[str] = ""  # empty string = delete
    # file_move
    new_path: Optional[str] = None
    # file_list
    recursive: Optional[bool] = False
    pattern: Optional[str] = None
    # command
    command: Optional[str] = None
    node: Optional[str] = None  # vps1 | vps2 | windows-desktop | macbook | any registered node name
    timeout: Optional[int] = 30
    # scaffold_query
    intent_tokens: Optional[list] = None
    atom_types: Optional[list] = None
    context_path: Optional[str] = None
    project: Optional[str] = None
    recent_types: Optional[list] = None
    limit: Optional[int] = 10
    # metadata
    session_id: Optional[str] = "helix_action"
    title: Optional[str] = None


class ActionResponse(BaseModel):
    status: str
    type: str
    result: Dict[str, Any]
    scaffold: Optional[Dict[str, Any]] = None


# ================================================================
# MAIN ENDPOINT
# ================================================================

@router.post("/action")
async def helix_action(req: ActionRequest):
    """
    Unified action endpoint. One tool. Helix routes everything.
    """
    action_type = req.type.lower().strip()

    try:
        if action_type == "file_write":
            return await _file_write(req)
        elif action_type == "file_patch":
            return await _file_patch(req)
        elif action_type == "file_move":
            return await _file_move(req)
        elif action_type == "file_delete":
            return await _file_delete(req)
        elif action_type == "file_read":
            return await _file_read(req)
        elif action_type == "file_list":
            return await _file_list(req)
        elif action_type == "command":
            return await _command(req)
        elif action_type == "scaffold_query":
            return await _scaffold_query(req)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown action type: '{action_type}'. "
                       f"Valid types: file_write, file_patch, file_move, file_delete, "
                       f"file_read, file_list, command, scaffold_query"
            )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"helix_action error ({action_type}): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ================================================================
# FILE WRITE
# ================================================================

async def _file_write(req: ActionRequest) -> dict:
    if not req.path:
        raise HTTPException(status_code=400, detail="file_write requires 'path'")
    if req.content is None:
        raise HTTPException(status_code=400, detail="file_write requires 'content'")

    # Route to remote node if specified
    if req.node and req.node.lower() not in ("vps1", "", "none"):
        result = await _node_file_write(req.node, req.path, req.content, req.session_id or "helix_action")
        if result is not None:
            return {"status": result.get("status", "ok"), "type": "file_write", "result": result, "scaffold": None}

    from services.workbench import get_workbench
    wb = get_workbench()

    result = await wb.write(
        path=req.path,
        content=req.content,
        title=req.title,
        project=req.project,
        session_id=req.session_id,
    )

    return {
        "status": result.get("status", "complete"),
        "type": "file_write",
        "result": result,
        "scaffold": None,  # Phase 1d: scaffold assembler
    }


# ================================================================
# FILE PATCH (str_replace)
# ================================================================

async def _file_patch(req: ActionRequest) -> dict:
    if not req.path:
        raise HTTPException(status_code=400, detail="file_patch requires 'path'")
    if req.old_str is None:
        raise HTTPException(status_code=400, detail="file_patch requires 'old_str'")

    from services.workbench import get_workbench, edit_file
    wb = get_workbench()

    # Use edit_file for the patch operation
    edit_result = edit_file(
        path=req.path,
        operations=[{"op": "replace", "old": req.old_str, "new": req.new_str or ""}]
    )

    if edit_result["status"] == "error":
        raise HTTPException(status_code=400, detail=edit_result.get("errors", [str(edit_result)]))

    # If edit succeeded, write the patched content through the full pipeline
    if edit_result.get("content") and edit_result["status"] in ("edited",):
        write_result = await wb.write(
            path=req.path,
            content=edit_result["content"],
            session_id=req.session_id,
            project=req.project,
        )
        edit_result["pipeline"] = write_result.get("steps", {})

    return {
        "status": edit_result.get("status", "ok"),
        "type": "file_patch",
        "result": edit_result,
        "scaffold": None,
    }


# ================================================================
# FILE MOVE
# ================================================================

async def _file_move(req: ActionRequest) -> dict:
    if not req.path:
        raise HTTPException(status_code=400, detail="file_move requires 'path'")
    if not req.new_path:
        raise HTTPException(status_code=400, detail="file_move requires 'new_path'")

    from services.workbench import get_workbench
    wb = get_workbench()

    result = await wb.move_file(req.path, req.new_path)

    # Log to observer
    wb.log_activity("move", req.new_path, {"from": req.path, "to": req.new_path}, req.session_id)

    return {
        "status": result.get("status", "moved"),
        "type": "file_move",
        "result": result,
        "scaffold": None,
    }


# ================================================================
# FILE DELETE
# ================================================================

async def _file_delete(req: ActionRequest) -> dict:
    if not req.path:
        raise HTTPException(status_code=400, detail="file_delete requires 'path'")

    from services.workbench import get_workbench
    wb = get_workbench()

    result = wb.delete_file(req.path)

    return {
        "status": result.get("status", "deleted"),
        "type": "file_delete",
        "result": result,
        "scaffold": None,
    }


# ================================================================
# FILE READ
# ================================================================

async def _file_read(req: ActionRequest) -> dict:
    if not req.path:
        raise HTTPException(status_code=400, detail="file_read requires 'path'")

    from services.workbench import get_workbench
    wb = get_workbench()

    result = wb.read_file(req.path, session_id=req.session_id)

    return {
        "status": result.get("status", "ok"),
        "type": "file_read",
        "result": result,
        "scaffold": None,
    }


# ================================================================
# FILE LIST
# ================================================================

async def _file_list(req: ActionRequest) -> dict:
    if not req.path:
        raise HTTPException(status_code=400, detail="file_list requires 'path'")

    from services.workbench import get_workbench
    wb = get_workbench()

    result = wb.list_dir(req.path, recursive=req.recursive or False)

    return {
        "status": result.get("status", "ok"),
        "type": "file_list",
        "result": result,
        "scaffold": None,
    }


# ================================================================
# COMMAND
# ================================================================

async def _command(req: ActionRequest) -> dict:
    if not req.command:
        raise HTTPException(status_code=400, detail="command requires 'command'")

    from services.workbench import run_shell

    result = run_shell(
        command=req.command,
        timeout=req.timeout or 30,
    )

    return {
        "status": result.get("status", "ok"),
        "type": "command",
        "result": result,
        "scaffold": None,
    }


# ================================================================
# SCAFFOLD QUERY (Pipeline B stub — Phase 1d)
# ================================================================

async def _scaffold_query(req: ActionRequest) -> dict:
    """
    Query atom store for scaffold context.
    Phase 1d: Returns matching atoms for intent tokens.
    Intent parser, dependency cascader, and assembler built in Phase 1d.
    """
    intent_tokens = req.intent_tokens or []
    context_path = req.context_path or req.path or ""
    limit = req.limit or 10

    try:
        from services import pg_sync
        conn = pg_sync.sqlite_conn()

        # Simple atom search by name matching intent tokens
        results = []
        for token in intent_tokens[:5]:  # cap at 5 tokens
            rows = conn.execute(
                """SELECT id, name, atom_type, code, language, structural_fp
                   FROM atoms
                   WHERE name ILIKE %s OR atom_type ILIKE %s
                   LIMIT %s""",
                (f"%{token}%", f"%{token}%", limit)
            ).fetchall()
            for row in rows:
                results.append(dict(row))

        # Deduplicate by id
        seen = set()
        unique = []
        for r in results:
            if r["id"] not in seen:
                seen.add(r["id"])
                unique.append(r)

        scaffold = {
            "atoms_matched": len(unique),
            "confidence": min(0.9, len(unique) / 10) if unique else 0.0,
            "atoms": unique[:limit],
            "imports": [],       # Phase 1d: scaffold assembler
            "boilerplate": "",   # Phase 1d: scaffold assembler
            "related_files": [], # Phase 1d: dependency cascader
            "note": "Phase 1d: intent parser + assembler + cascader building soon"
        }

        return {
            "status": "ok",
            "type": "scaffold_query",
            "result": {"intent_tokens": intent_tokens, "context_path": context_path},
            "scaffold": scaffold,
        }

    except Exception as e:
        log.warning(f"scaffold_query failed: {e}")
        return {
            "status": "ok",
            "type": "scaffold_query",
            "result": {"intent_tokens": intent_tokens},
            "scaffold": {
                "atoms_matched": 0,
                "confidence": 0.0,
                "atoms": [],
                "note": f"scaffold_query stub: {str(e)}"
            },
        }
