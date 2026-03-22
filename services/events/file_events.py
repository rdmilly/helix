"""File Event Handlers — Subscribers for file.written and file.read

All subscribers run concurrently via asyncio.gather().
Individual subscriber failures are logged but never raise —
the write already succeeded; enrichment is best-effort.

Subscribers for file.written (respects 'steps' flags from payload):
  - versioner    : MinIO versioning via Forge
  - scanner      : AST atom extraction (code files only)
  - kb_indexer   : FTS5 KB indexing (doc/config files only)
  - kg_extractor : Entity/relationship extraction
  - observer     : Activity log
  - shard_differ : Diff against previous version
  - snapshot     : Queue component snapshot (code files only)

Subscribers for file.read:
  - access_log   : Record the read in observer
  - predictor    : Update meta.co_occurrence (write-on-touch)
"""
import asyncio
import logging
from typing import Any, Dict

log = logging.getLogger("helix.events.file")


async def handle_file_written(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Dispatch all file.written subscribers concurrently.

    Respects the 'steps' dict in payload to skip disabled subscribers.
    This dict is set by the workbench based on file type and caller flags.
    """
    path = payload.get("path", "")
    content = payload.get("content", "")
    file_type = payload.get("file_type", "unknown")
    session_id = payload.get("session_id", "workbench")
    project = payload.get("project")
    title = payload.get("title")

    # Step flags from workbench (version/scan/index/entities/observer/snapshot)
    steps = payload.get("steps", {})
    do_version  = steps.get("version", True)
    do_scan     = steps.get("scan", file_type in ("code", "dockerfile"))
    do_index    = steps.get("index", file_type in ("doc", "config", "compose"))
    do_entities = steps.get("entities", True)
    do_observer = steps.get("observer", True)
    do_snapshot = steps.get("snapshot", file_type in ("code", "dockerfile"))
    # Shard diff always runs — not controlled by legacy flags
    do_shard    = True

    log.info(f"file.written dispatching: {path} ({file_type})")

    from services.workbench import get_workbench
    wb = get_workbench()

    tasks = []
    labels = []

    if do_version:
        tasks.append(wb.version_file(path, content, project))
        labels.append("versioner")

    if do_scan:
        tasks.append(wb.scan_code(content, path))
        labels.append("scanner")

    if do_index:
        tasks.append(asyncio.to_thread(wb.index_kb, path, content, title, "workbench"))
        labels.append("kb_indexer")

    if do_entities:
        tasks.append(asyncio.to_thread(wb.extract_entities, content, path, session_id))
        labels.append("kg_extractor")

    if do_observer:
        tasks.append(asyncio.to_thread(
            wb.log_activity, "write", path,
            {"file_type": file_type, "size": len(content.encode()), "event_driven": True},
            session_id
        ))
        labels.append("observer")

    if do_shard:
        tasks.append(_run_shard_diff(path, content, session_id))
        labels.append("shard_differ")

    if do_snapshot:
        tasks.append(_queue_snapshot(path, session_id))
        labels.append("snapshot")

    # Site updater: refresh helixmaster commit feed on every write
    tasks.append(asyncio.to_thread(_update_site))
    labels.append("site_updater")

    if not tasks:
        return {"event": "file.written", "path": path, "subscribers": 0, "results": {}}

    # Run all subscribers concurrently
    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results = {}
    for label, result in zip(labels, raw_results):
        if isinstance(result, Exception):
            log.warning(f"file.written subscriber '{label}' failed: {result}")
            results[label] = {"status": "error", "error": str(result)}
        else:
            results[label] = result if isinstance(result, dict) else {"status": "ok"}

    log.info(f"file.written complete: {path} — {len(tasks)} subscribers ran")
    return {
        "event": "file.written",
        "path": path,
        "file_type": file_type,
        "subscribers": len(tasks),
        "results": results,
    }


async def handle_file_read(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Dispatch file.read subscribers.

    Runs in background after helix_file_read returns to caller.
    Write-on-touch: reading a file triggers epigenetic enrichment.
    """
    path = payload.get("path", "")
    file_type = payload.get("file_type", "unknown")
    session_id = payload.get("session_id", "workbench")

    from services.workbench import get_workbench
    wb = get_workbench()

    tasks = []
    labels = []

    # Access log
    tasks.append(asyncio.to_thread(
        wb.log_activity, "read", path,
        {"file_type": file_type, "size_bytes": payload.get("size_bytes", 0)},
        session_id
    ))
    labels.append("access_log")

    # Predictor: write-on-touch co_occurrence update
    tasks.append(_update_cooccurrence(path, session_id, file_type))
    labels.append("predictor")

    # Re-fingerprinter: detect and repair stale fp_version atoms on code files
    if file_type in ("code", "dockerfile"):
        tasks.append(_check_fingerprints(path, session_id))
        labels.append("re_fingerprinter")

    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results = {}
    for label, result in zip(labels, raw_results):
        if isinstance(result, Exception):
            log.debug(f"file.read subscriber '{label}' failed: {result}")
            results[label] = {"status": "error", "error": str(result)}
        else:
            results[label] = result if isinstance(result, dict) else {"status": "ok"}

    return {"event": "file.read", "path": path, "results": results}


# ================================================================
# Internal helpers
# ================================================================

async def _run_shard_diff(path: str, content: str, session_id: str) -> Dict[str, Any]:
    """Run shard diff for this file write.

    Compares new content against previous version in MinIO.
    Non-fatal on any error — diff is enrichment, not blocking.
    """
    try:
        from services.shard import get_shard_service
        shard = get_shard_service()
        # diff_on_write: compare new content vs last MinIO version
        result = await asyncio.to_thread(shard.diff_on_write, path, content, session_id)
        return result if isinstance(result, dict) else {"status": "ok"}
    except AttributeError:
        # diff_on_write not yet implemented in shard service
        log.debug(f"shard.diff_on_write not available yet, skipping")
        return {"status": "pending_implementation"}
    except Exception as e:
        log.debug(f"Shard diff skipped for {path}: {e}")
        return {"status": "skipped", "reason": str(e)}


async def _queue_snapshot(path: str, session_id: str) -> Dict[str, Any]:
    """Queue a component snapshot for code files."""
    try:
        from services.snapshots import queue_snapshot_for_file
        component = await asyncio.to_thread(queue_snapshot_for_file, path, session_id)
        return {"status": "queued", "component": component} if component else {"status": "no_match"}
    except Exception as e:
        log.debug(f"Snapshot queue skipped: {e}")
        return {"status": "skipped", "reason": str(e)}


async def _update_cooccurrence(path: str, session_id: str, file_type: str) -> Dict[str, Any]:
    """Phase 2 write-on-touch co_occurrence enrichment.

    Algorithm:
    1. Read paths accessed in this session from session meta.
    2. Record this path access.
    3. Find atoms that live in this file (via meta.provenance LIKE stem).
    4. Find atoms in co-accessed files.
    5. Write meta.co_occurrence on each current-file atom.
    """
    import json as _json
    try:
        from services.meta import get_meta_service
        from services.database import get_db
        from pathlib import Path as _Path

        if file_type not in ("code", "dockerfile"):
            return {"status": "skipped", "reason": "not a code file"}

        meta_svc = get_meta_service()
        db = get_db()
        stem = _Path(path).stem

        # Step 1+2: update session access log
        co_paths = []
        try:
            existing_access = meta_svc.read_meta("sessions", session_id, "file_access")
            existing_paths = existing_access.get("paths", [])
            co_paths = [p for p in existing_paths if p != path]
            if path not in existing_paths:
                existing_paths.append(path)
            meta_svc.write_meta("sessions", session_id, "file_access",
                {"paths": existing_paths, "count": len(existing_paths)},
                written_by="file_events.predictor_v1")
        except Exception:
            pass  # session may not exist yet

        # Step 3: atoms in this file (match via provenance stem)
        with db.get_connection() as conn:
            current_atoms = conn.execute(
                'SELECT id, name FROM atoms WHERE meta LIKE ?',
                (f'"%{stem}%"',)
            ).fetchall()

            # Step 4: atoms in co-accessed files (last 5 paths)
            co_atom_names = set()
            for co_path in co_paths[-5:]:
                co_stem = _Path(co_path).stem
                rows = conn.execute(
                    'SELECT name FROM atoms WHERE meta LIKE ?',
                    (f'"%{co_stem}%"',)
                ).fetchall()
                co_atom_names.update(r[0] for r in rows)

        if not current_atoms:
            return {"status": "no_atoms", "path": path}

        if not co_atom_names:
            return {"status": "ok", "atoms_found": len(current_atoms), "co_atoms": 0}

        # Step 5: write meta.co_occurrence on each current atom
        updated = 0
        for atom_id, atom_name in current_atoms:
            try:
                existing = meta_svc.read_meta("atoms", atom_id, "co_occurrence")
                always_with = existing.get("always_with", [])
                observations = existing.get("observations", 0) + 1
                for name in co_atom_names:
                    if name != atom_name and name not in always_with:
                        always_with.append(name)
                always_with = always_with[:20]  # cap list
                confidence = round(min(0.99, observations / (observations + 5)), 3)
                meta_svc.write_meta("atoms", atom_id, "co_occurrence", {
                    "always_with": always_with,
                    "confidence": confidence,
                    "observations": observations,
                    "last_seen_with": list(co_atom_names)[:5],
                }, written_by="predictor_v1")
                updated += 1
            except Exception:
                pass

        return {"status": "ok", "atoms_updated": updated, "co_atoms": len(co_atom_names)}

    except Exception as e:
        log.debug(f"co_occurrence update failed: {e}")
        return {"status": "skipped", "reason": str(e)}


async def _check_fingerprints(path: str, session_id: str) -> Dict[str, Any]:
    """On-touch re-fingerprinting: find atoms in this file with stale fp_version.

    Stale atoms are re-fingerprinted using the current algorithm version.
    This is the organic migration path from the epigenetic spec.
    """
    try:
        from config import CURRENT_FP_VERSION
        from services.scanner import get_scanner_service
        from services.database import get_db
        from pathlib import Path as _Path

        stem = _Path(path).stem
        db = get_db()

        with db.get_connection() as conn:
            stale = conn.execute(
                'SELECT id, name, code FROM atoms WHERE meta LIKE ? AND fp_version != ? LIMIT 10',
                (f'"%{stem}%"', CURRENT_FP_VERSION)
            ).fetchall()

        if not stale:
            return {"status": "current", "path": path}

        scanner = get_scanner_service()
        recomputed = 0
        for atom_id, atom_name, code in stale:
            try:
                sfp = await scanner.compute_structural_fingerprint(code)
                sem_fp, _ = await scanner.compute_semantic_fingerprint(code)
                with db.get_connection() as conn:
                    conn.execute(
                        "UPDATE atoms SET structural_fp = ?, semantic_fp = ?, fp_version = ? WHERE id = ?",
                        (sfp, sem_fp, CURRENT_FP_VERSION, atom_id)
                    )
                    conn.commit()
                recomputed += 1
                log.debug(f"Re-fingerprinted {atom_id} ({atom_name}) -> {CURRENT_FP_VERSION}")
            except Exception:
                pass

        return {"status": "ok", "stale_found": len(stale), "recomputed": recomputed}

    except Exception as e:
        log.debug(f"re_fingerprinter skipped: {e}")
        return {"status": "skipped", "reason": str(e)}
