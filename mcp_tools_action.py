"""helix_action MCP Tools — File Operations + Commands

Appended to mcp_tools.py via import.
Exposes helix_action as individual MCP tools so Claude can call them directly.
All tools delegate to routers/action.py logic via workbench service.

Tools:
  helix_file_write    — write file, full pipeline
  helix_file_patch    — str_replace edit, full pipeline
  helix_file_read     — read file
  helix_file_list     — list directory
  helix_file_move     — move file, KG path label update
  helix_file_delete   — delete file, observer log
  helix_command       — run shell command
  helix_scaffold_query — query atom store for scaffold context
"""

import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _register_action_tools(mcp):
    """Register all helix_action tools onto the given FastMCP instance."""

    @mcp.tool(
        name="file_write",
        annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True},
    )
    async def helix_file_write(
        path: str,
        content: str,
        session_id: Optional[str] = "helix_action",
        project: Optional[str] = None,
        title: Optional[str] = None,
    ) -> str:
        """Write a file to disk and run the full enrichment pipeline.

        Pipeline A (async): version → atom scan → KB index → KG update → observer log.
        Pipeline B: scaffold context returned alongside result.

        Use this for ALL file writes. Replaces workspace_write and ssh_write_file.

        Args:
            path: Absolute path on VPS1 (e.g. /opt/projects/helix/routers/action.py)
            content: Full file content to write
            session_id: Optional session identifier for observer log
            project: Optional project name for versioning context
            title: Optional title for KB indexing
        Returns:
            JSON with write status, pipeline steps, file hash, and scaffold context.
        """
        try:
            from services.workbench import get_workbench
            wb = get_workbench()
            result = await wb.write(
                path=path, content=content,
                title=title, project=project,
                session_id=session_id or "helix_action",
            )
            return json.dumps({"status": result.get("status"), "path": path, "steps": result.get("steps"), "file_type": result.get("file_type"), "scaffold": None, "note": result.get("note", "")}, indent=2, default=str)
        except Exception as e:
            logger.error(f"helix_file_write failed: {e}")
            return json.dumps({"status": "error", "error": str(e), "path": path})


    @mcp.tool(
        name="file_patch",
        annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
    )
    async def helix_file_patch(
        path: str,
        old_str: str,
        new_str: str = "",
        session_id: Optional[str] = "helix_action",
    ) -> str:
        """Make a targeted str_replace edit to an existing file.

        Reads the file, applies the replacement, writes back through full pipeline.
        old_str must appear exactly once in the file.
        new_str = "" to delete the matched section.

        Args:
            path: Absolute path on VPS1
            old_str: Exact string to find and replace (must be unique in file)
            new_str: Replacement string (empty string to delete)
            session_id: Optional session identifier
        Returns:
            JSON with edit status, before/after sizes, and pipeline result.
        """
        try:
            from services.workbench import get_workbench, edit_file
            wb = get_workbench()

            edit_result = edit_file(path=path, operations=[{"op": "replace", "old": old_str, "new": new_str}])

            if edit_result["status"] == "error":
                return json.dumps({"status": "error", "errors": edit_result.get("errors"), "path": path})

            if edit_result.get("content") and edit_result["status"] == "edited":
                write_result = await wb.write(path=path, content=edit_result["content"], session_id=session_id or "helix_action")
                edit_result["pipeline"] = write_result.get("steps", {})

            del edit_result["content"]  # don't return full content
            return json.dumps({"status": edit_result.get("status"), "path": path, "applied": edit_result.get("applied"), "delta_bytes": edit_result.get("delta_bytes"), "pipeline": edit_result.get("pipeline")}, indent=2, default=str)
        except Exception as e:
            logger.error(f"helix_file_patch failed: {e}")
            return json.dumps({"status": "error", "error": str(e), "path": path})


    @mcp.tool(
        name="file_read",
        annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True},
    )
    async def helix_file_read(
        path: str,
        session_id: Optional[str] = "helix_action",
    ) -> str:
        """Read a file from VPS1 disk.

        Publishes a file.read event for co-occurrence enrichment (write-on-touch).
        No pipeline runs — read-only.

        Args:
            path: Absolute path on VPS1
            session_id: Optional session identifier
        Returns:
            JSON with file content, size, and file type classification.
        """
        try:
            from services.workbench import get_workbench
            wb = get_workbench()
            result = wb.read_file(path, session_id=session_id or "helix_action")
            return json.dumps(result, indent=2, default=str)
        except Exception as e:
            return json.dumps({"status": "error", "error": str(e), "path": path})


    @mcp.tool(
        name="file_list",
        annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True},
    )
    async def helix_file_list(
        path: str,
        recursive: bool = False,
    ) -> str:
        """List directory contents on VPS1.

        Args:
            path: Absolute directory path on VPS1
            recursive: Whether to list recursively (default: False)
        Returns:
            JSON with file/directory listing including types and sizes.
        """
        try:
            from services.workbench import get_workbench
            wb = get_workbench()
            result = wb.list_dir(path, recursive=recursive)
            return json.dumps(result, indent=2, default=str)
        except Exception as e:
            return json.dumps({"status": "error", "error": str(e), "path": path})


    @mcp.tool(
        name="file_move",
        annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
    )
    async def helix_file_move(
        path: str,
        new_path: str,
        session_id: Optional[str] = "helix_action",
    ) -> str:
        """Move or rename a file on VPS1 disk.

        Updates KG path labels by content hash — epigenetic identity preserved.
        No data migration needed: content hash is identity, path is just a label.

        Args:
            path: Source path (absolute)
            new_path: Destination path (absolute)
            session_id: Optional session identifier
        Returns:
            JSON with move status.
        """
        try:
            from services.workbench import get_workbench
            wb = get_workbench()
            result = await wb.move_file(path, new_path)
            return json.dumps(result, indent=2, default=str)
        except Exception as e:
            return json.dumps({"status": "error", "error": str(e), "path": path})


    @mcp.tool(
        name="file_delete",
        annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False},
    )
    async def helix_file_delete(
        path: str,
        session_id: Optional[str] = "helix_action",
    ) -> str:
        """Delete a file from VPS1 disk.

        Logs deletion to observer so it's never invisible.

        Args:
            path: Absolute path to delete
            session_id: Optional session identifier
        Returns:
            JSON with deletion status.
        """
        try:
            from services.workbench import get_workbench
            wb = get_workbench()
            result = wb.delete_file(path)
            wb.log_activity("delete", path, session_id=session_id or "helix_action")
            return json.dumps(result, indent=2, default=str)
        except Exception as e:
            return json.dumps({"status": "error", "error": str(e), "path": path})


    @mcp.tool(
        name="command",
        annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
    )
    async def helix_command(
        command: str,
        cwd: Optional[str] = None,
        timeout: int = 30,
        session_id: Optional[str] = "helix_action",
    ) -> str:
        """Run a shell command on VPS1 (inside helix-cortex container context).

        Captures stdout, stderr, exit code. Logs to observer.
        For docker/systemctl on VPS host, use gateway__ssh_execute.
        For cross-server commands, use gateway__ssh_execute.

        Args:
            command: Shell command to run
            cwd: Working directory (optional)
            timeout: Max seconds to wait (default 30)
            session_id: Optional session identifier
        Returns:
            JSON with exit_code, stdout, stderr, duration_ms.
        """
        try:
            from services.workbench import run_shell
            result = run_shell(command=command, cwd=cwd, timeout=timeout)
            return json.dumps(result, indent=2, default=str)
        except Exception as e:
            return json.dumps({"status": "error", "error": str(e), "command": command})


    @mcp.tool(
        name="scaffold_query",
        annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True},
    )
    async def helix_scaffold_query(
        intent_tokens: Optional[list] = None,
        context_path: Optional[str] = None,
        project: Optional[str] = None,
        recent_types: Optional[list] = None,
        limit: int = 10,
    ) -> str:
        """Query the atom store for scaffold context.

        Used by MemBrain pre-generation injection to assemble scaffold
        before the LLM generates. Returns matching atoms, boilerplate,
        imports, and related files that need updating.

        Phase 1d stub: returns atom matches. Full assembler + cascader building soon.

        Args:
            intent_tokens: Tokens extracted from message intent (e.g. ["router", "invoice"])
            context_path: Current file/directory context
            project: Project name to bias retrieval
            recent_types: Recently touched atom types for recency bias
            limit: Max atoms to return
        Returns:
            JSON scaffold object with atoms_matched, confidence, atoms[], imports[], boilerplate, related_files[].
        """
        intent_tokens = intent_tokens or []
        limit = max(1, min(limit, 20))

        try:
            from services import pg_sync
            conn = pg_sync.get_connection()

            results = []
            for token in intent_tokens[:5]:
                rows = conn.execute(
                    """SELECT id, name, atom_type, language
                       FROM atoms
                       WHERE name ILIKE %s OR atom_type ILIKE %s
                       LIMIT %s""",
                    (f"%{token}%", f"%{token}%", limit)
                ).fetchall()
                for row in rows:
                    results.append({"id": row[0], "name": row[1], "atom_type": row[2], "language": row[3]})

            # Deduplicate
            seen = set()
            unique = []
            for r in results:
                if r["id"] not in seen:
                    seen.add(r["id"])
                    unique.append(r)

            scaffold = {
                "atoms_matched": len(unique),
                "confidence": round(min(0.9, len(unique) / max(limit, 1)), 3),
                "atoms": unique[:limit],
                "imports": [],
                "boilerplate": "",
                "related_files": [],
                "intent_tokens": intent_tokens,
                "context_path": context_path,
                "note": "Phase 1d: assembler + cascader building — returning raw atom matches now"
            }

            return json.dumps(scaffold, indent=2, default=str)
        except Exception as e:
            logger.warning(f"helix_scaffold_query failed: {e}")
            return json.dumps({"atoms_matched": 0, "confidence": 0.0, "atoms": [], "error": str(e)})
