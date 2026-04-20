"""MemBrain Backfill Router — /api/v1/backfill/*

Provides endpoints for the MemBrain extension to progressively backfill
historical claude.ai conversation data into MinIO, tracked by Helix state.

Architecture:
    - Extension runs in user's browser, authenticated to claude.ai
    - Extension fetches /api/organizations/{org}/chat_conversations list
      + individual conversation payloads and POSTs them here
    - Helix parses, extracts artifacts/code/tools/thinking/attachments,
      writes to MinIO `archive` bucket, and updates `backfill_state` table
      so the extension knows what's done vs pending on next session

Size policy:
    - Code/text content: NO size cap (artifacts, code blocks, message text,
      tool_use args/results, thinking blocks, json, yaml, md, etc.)
    - Non-code uploads (PDFs, images, video, audio, binary): 25 MB per object

Endpoints:
    GET    /api/v1/backfill/state
               Returns summary + list of UUIDs the extension should target next.
    POST   /api/v1/backfill/seen
               Extension reports conversation UUIDs it discovered via list endpoint.
    POST   /api/v1/backfill/ingest
               Extension uploads one conversation's full payload. We parse,
               extract, write to MinIO, mark harvested.
    POST   /api/v1/backfill/attachment
               Extension uploads one attachment blob (base64 or raw bytes).
               Size-capped for non-code types.
    GET    /api/v1/backfill/stats
               Totals + per-type breakdown for the HUD status pill.

Auth: Bearer token via existing membrain_auth. Also supports
X-Helix-Admin-Key for internal / manual runs.
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Optional

import boto3
from botocore.client import Config as BotoConfig
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

from services import pg_sync
from services.database import get_db_path
from services.membrain_auth import membrain_auth, MembrainUser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/backfill", tags=["Backfill"])

# --- Config -----------------------------------------------------------------

ADMIN_KEY = os.getenv("HELIX_ADMIN_KEY", "")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "https://s3.millyweb.com")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")
MINIO_BUCKET = os.getenv("BACKFILL_BUCKET", "archive")

ATTACHMENT_SIZE_CAP_BYTES = 25 * 1024 * 1024  # 25 MB for non-code uploads

# Code-like extensions / content-types (bypass size cap)
CODE_EXTENSIONS = {
    "py", "js", "jsx", "ts", "tsx", "html", "htm", "css", "scss", "sass",
    "json", "yaml", "yml", "toml", "xml", "md", "rst", "txt",
    "sh", "bash", "zsh", "fish", "ps1", "bat",
    "go", "rs", "rb", "php", "java", "kt", "swift", "c", "cpp", "h", "hpp",
    "cs", "scala", "clj", "ex", "exs", "lua", "r", "pl", "pm", "dart", "nim",
    "sql", "dockerfile", "makefile", "gradle", "conf", "cfg", "ini",
    "env", "gitignore", "gitattributes", "editorconfig",
    "vue", "svelte", "astro", "mjs", "cjs",
    "graphql", "gql", "proto", "tf", "hcl", "bicep",
}

# --- Auth -------------------------------------------------------------------

async def require_auth(
    authorization: Optional[str] = Header(None),
    x_helix_admin_key: Optional[str] = Header(None),
) -> dict:
    """Accept either a MemBrain user token or the admin key. Returns dict with
    user info for downstream logging."""
    if x_helix_admin_key and ADMIN_KEY and x_helix_admin_key == ADMIN_KEY:
        return {"kind": "admin", "user_id": "admin"}
    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:]
        user = await membrain_auth.verify_token(token)
        if user and not user.revoked:
            return {"kind": "user", "user_id": user.id, "email": user.email}
    raise HTTPException(401, "unauthorized")


# --- MinIO ------------------------------------------------------------------

def _s3():
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise HTTPException(500, "backfill MinIO credentials not configured")
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
    )


def _put(key: str, body: bytes | str, content_type: str = "application/octet-stream") -> None:
    if isinstance(body, str):
        body = body.encode("utf-8")
    _s3().put_object(Bucket=MINIO_BUCKET, Key=key, Body=body, ContentType=content_type)


# --- State (Postgres) --------------------------------------------------------

def _pg():
    return pg_sync.pg_conn()


def ensure_schema() -> None:
    """Create backfill_state table if missing. Idempotent; safe to call on startup."""
    ddl = """
    CREATE TABLE IF NOT EXISTS backfill_state (
        conv_uuid       TEXT PRIMARY KEY,
        org_uuid        TEXT,
        status          TEXT NOT NULL DEFAULT 'seen',  -- seen | harvested | failed
        first_seen      TIMESTAMPTZ NOT NULL DEFAULT now(),
        harvested_at    TIMESTAMPTZ,
        error           TEXT,
        message_count   INTEGER DEFAULT 0,
        artifact_count  INTEGER DEFAULT 0,
        code_block_count INTEGER DEFAULT 0,
        tool_call_count INTEGER DEFAULT 0,
        thinking_count  INTEGER DEFAULT 0,
        attachment_total INTEGER DEFAULT 0,
        attachment_done INTEGER DEFAULT 0,
        payload_bytes   BIGINT DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_backfill_state_status ON backfill_state(status);

    CREATE TABLE IF NOT EXISTS backfill_attachments (
        id              TEXT PRIMARY KEY,
        conv_uuid       TEXT NOT NULL,
        kind            TEXT NOT NULL,  -- 'file' | 'image'
        source_url      TEXT,
        file_name       TEXT,
        content_type    TEXT,
        size_bytes      BIGINT,
        is_code_like    BOOLEAN DEFAULT FALSE,
        status          TEXT NOT NULL DEFAULT 'pending',  -- pending | stored | skipped_oversize | failed
        minio_key       TEXT,
        sha256          TEXT,
        first_seen      TIMESTAMPTZ NOT NULL DEFAULT now(),
        stored_at       TIMESTAMPTZ,
        error           TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_backfill_attachments_conv ON backfill_attachments(conv_uuid);
    CREATE INDEX IF NOT EXISTS idx_backfill_attachments_status ON backfill_attachments(status);
    """
    with _pg() as conn, conn.cursor() as cur:
        for stmt in ddl.split(";"):
            if stmt.strip():
                cur.execute(stmt)
        conn.commit()


# Run once on import
try:
    ensure_schema()
except Exception as e:
    logger.warning("backfill schema ensure failed: %s (will retry on first request)", e)


# --- Parsing helpers --------------------------------------------------------

ARTIFACT_RE = re.compile(r'<antArtifact\s+([^>]*)>(.*?)</antArtifact>', re.DOTALL)
ATTR_RE = re.compile(r'(\w+)="([^"]*)"')
CODE_FENCE_RE = re.compile(r'```(\w+)?\n(.*?)```', re.DOTALL)


def is_code_like(filename: str = "", content_type: str = "") -> bool:
    """Determine whether an attachment is code/text (bypasses size cap) or binary."""
    ct = (content_type or "").lower()
    if ct.startswith("text/") or ct in ("application/json", "application/xml", "application/yaml"):
        return True
    if ct.startswith("image/") or ct.startswith("video/") or ct.startswith("audio/"):
        return False
    fn = (filename or "").lower()
    if "." in fn:
        ext = fn.rsplit(".", 1)[-1]
        if ext in CODE_EXTENSIONS:
            return True
    return False


def parse_artifacts(text: str) -> list[dict]:
    out = []
    for m in ARTIFACT_RE.finditer(text):
        attrs = dict(ATTR_RE.findall(m.group(1)))
        content = m.group(2)
        out.append({
            "identifier": attrs.get("identifier", f"unknown-{len(out)}"),
            "type": attrs.get("type", "text/plain"),
            "title": attrs.get("title", ""),
            "language": attrs.get("language", ""),
            "content": content,
            "content_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
            "content_length": len(content),
        })
    return out


def parse_code_blocks(text: str) -> list[dict]:
    out = []
    for m in CODE_FENCE_RE.finditer(text):
        lang = m.group(1) or "text"
        content = m.group(2)
        if len(content.strip()) < 20:
            continue
        out.append({
            "language": lang,
            "content": content,
            "content_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
            "content_length": len(content),
        })
    return out


def extract_message_blocks(msg: dict) -> tuple[str, list[dict], list[dict], list[dict]]:
    content = msg.get("content", [])
    if isinstance(content, str):
        return content, [], [], []
    if not isinstance(content, list):
        return "", [], [], []
    text_parts, tool_uses, tool_results, thinking = [], [], [], []
    for block in content:
        if not isinstance(block, dict):
            continue
        btype = block.get("type", "")
        if btype == "text":
            text_parts.append(block.get("text", ""))
        elif btype == "tool_use":
            tool_uses.append({
                "id": block.get("id", ""), "name": block.get("name", ""),
                "input": block.get("input", {}),
            })
        elif btype == "tool_result":
            tool_results.append({
                "tool_use_id": block.get("tool_use_id", ""),
                "content": block.get("content", ""),
                "is_error": block.get("is_error", False),
            })
        elif btype == "thinking":
            thinking.append({
                "text": block.get("thinking", "") or block.get("text", ""),
                "signature": block.get("signature", ""),
            })
        else:
            text_parts.append(json.dumps(block))
    return "\n".join(text_parts), tool_uses, tool_results, thinking


def extension_for_artifact(a: dict) -> str:
    atype = a.get("type", "")
    lang = (a.get("language") or "").lower()
    if "react" in atype: return "jsx"
    if "html" in atype: return "html"
    if "svg" in atype: return "svg"
    if "markdown" in atype or lang == "markdown": return "md"
    if "mermaid" in atype: return "mermaid"
    mapping = {"python": "py", "py": "py", "javascript": "js", "js": "js",
               "typescript": "ts", "ts": "ts", "bash": "sh", "sh": "sh",
               "shell": "sh", "json": "json", "yaml": "yml", "yml": "yml",
               "go": "go", "rust": "rs", "sql": "sql"}
    return mapping.get(lang, "txt")


# --- Request models ---------------------------------------------------------

class SeenBatch(BaseModel):
    org_uuid: str
    uuids: list[str] = Field(default_factory=list)


class IngestRequest(BaseModel):
    conv_uuid: str
    org_uuid: str
    payload: dict  # raw claude.ai conversation payload


class AttachmentRequest(BaseModel):
    conv_uuid: str
    kind: str  # 'file' | 'image'
    source_url: str
    file_name: str = ""
    content_type: str = ""
    data_b64: Optional[str] = None  # if omitted, only metadata is stored
    message_index: int = -1


# --- Endpoints --------------------------------------------------------------

@router.get("/state")
async def get_state(_: dict = Depends(require_auth)) -> dict:
    """Return summary + next-up work for the extension."""
    with _pg() as conn, conn.cursor() as cur:
        cur.execute("SELECT status, COUNT(*) FROM backfill_state GROUP BY status")
        by_status = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute(
            "SELECT conv_uuid, org_uuid FROM backfill_state "
            "WHERE status = 'seen' ORDER BY first_seen ASC LIMIT 50"
        )
        next_harvest = [{"conv_uuid": r[0], "org_uuid": r[1]} for r in cur.fetchall()]
        cur.execute(
            "SELECT id, conv_uuid, source_url, file_name, content_type, size_bytes "
            "FROM backfill_attachments WHERE status = 'pending' "
            "ORDER BY first_seen ASC LIMIT 50"
        )
        next_attachments = [
            {"id": r[0], "conv_uuid": r[1], "source_url": r[2],
             "file_name": r[3], "content_type": r[4], "size_hint": r[5]}
            for r in cur.fetchall()
        ]
    return {
        "summary": by_status,
        "next_harvest": next_harvest,
        "next_attachments": next_attachments,
        "size_cap_bytes": ATTACHMENT_SIZE_CAP_BYTES,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/stats")
async def get_stats(_: dict = Depends(require_auth)) -> dict:
    """Compact stats for HUD. Returns totals only, no lists."""
    with _pg() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FILTER (WHERE status='seen') AS seen, "
            "       COUNT(*) FILTER (WHERE status='harvested') AS harvested, "
            "       COUNT(*) FILTER (WHERE status='failed') AS failed, "
            "       COALESCE(SUM(artifact_count),0) AS artifacts, "
            "       COALESCE(SUM(code_block_count),0) AS code_blocks, "
            "       COALESCE(SUM(tool_call_count),0) AS tool_calls, "
            "       COALESCE(SUM(thinking_count),0) AS thinking, "
            "       COALESCE(SUM(payload_bytes),0) AS total_bytes "
            "FROM backfill_state"
        )
        s = cur.fetchone()
        cur.execute(
            "SELECT COUNT(*) FILTER (WHERE status='pending') AS pending, "
            "       COUNT(*) FILTER (WHERE status='stored') AS stored, "
            "       COUNT(*) FILTER (WHERE status='skipped_oversize') AS oversize, "
            "       COUNT(*) FILTER (WHERE status='failed') AS failed "
            "FROM backfill_attachments"
        )
        a = cur.fetchone()
    return {
        "conversations": {
            "seen": s[0], "harvested": s[1], "failed": s[2],
            "artifacts": s[3], "code_blocks": s[4],
            "tool_calls": s[5], "thinking": s[6],
            "total_bytes": int(s[7] or 0),
        },
        "attachments": {
            "pending": a[0], "stored": a[1],
            "oversize": a[2], "failed": a[3],
        },
    }


@router.post("/seen")
async def report_seen(body: SeenBatch, _: dict = Depends(require_auth)) -> dict:
    """Extension reports UUIDs discovered via the list endpoint. Upsert into state."""
    inserted = 0
    with _pg() as conn, conn.cursor() as cur:
        for u in body.uuids:
            cur.execute(
                "INSERT INTO backfill_state (conv_uuid, org_uuid) VALUES (%s, %s) "
                "ON CONFLICT (conv_uuid) DO NOTHING",
                (u, body.org_uuid),
            )
            if cur.rowcount:
                inserted += 1
        conn.commit()
    return {"received": len(body.uuids), "new": inserted}


@router.post("/ingest")
async def ingest_conversation(body: IngestRequest, _: dict = Depends(require_auth)) -> dict:
    """Parse a conversation payload, write extracts to MinIO, mark harvested."""
    conv_uuid = body.conv_uuid
    full = body.payload
    prefix = f"conversations/{conv_uuid}"

    try:
        # 1. Raw payload — source of truth
        payload_bytes = json.dumps(full, separators=(",", ":")).encode("utf-8")
        _put(f"{prefix}/full.json", payload_bytes, "application/json")

        # 2. Metadata
        meta = {
            "uuid": conv_uuid,
            "name": full.get("name", ""),
            "summary": full.get("summary", ""),
            "created_at": full.get("created_at", ""),
            "updated_at": full.get("updated_at", ""),
            "model": full.get("model", ""),
            "project_uuid": (full.get("project") or {}).get("uuid"),
            "project_name": (full.get("project") or {}).get("name"),
            "message_count": len(full.get("chat_messages", [])),
        }
        _put(f"{prefix}/meta.json", json.dumps(meta, indent=2), "application/json")

        # 3. Walk messages
        messages = full.get("chat_messages", [])
        all_artifacts, all_code_blocks = [], []
        all_tool_uses, all_tool_results, all_thinking = [], [], []
        messages_flat, file_refs, image_refs = [], [], []

        for i, msg in enumerate(messages):
            sender = msg.get("sender", "")
            created = msg.get("created_at", "")
            text, tool_uses, tool_results, thinking = extract_message_blocks(msg)
            messages_flat.append({"index": i, "sender": sender,
                                  "created_at": created, "text": text})
            if sender == "assistant":
                for a in parse_artifacts(text):
                    a["message_index"] = i; a["created_at"] = created
                    all_artifacts.append(a)
                for b in parse_code_blocks(text):
                    b["message_index"] = i; b["created_at"] = created
                    all_code_blocks.append(b)
            for x in tool_uses: x["message_index"] = i; all_tool_uses.append(x)
            for x in tool_results: x["message_index"] = i; all_tool_results.append(x)
            for x in thinking: x["message_index"] = i; all_thinking.append(x)
            for f in msg.get("files", []) or []:
                file_refs.append({
                    "message_index": i,
                    "file_name": f.get("file_name", ""),
                    "file_size": f.get("file_size", 0),
                    "file_kind": f.get("file_kind", ""),
                    "content_type": f.get("file_kind") or "",
                    "url": f.get("document_asset") or f.get("preview_url") or f.get("thumbnail_url", ""),
                })
            for a in msg.get("attachments", []) or []:
                image_refs.append({
                    "message_index": i,
                    "file_name": a.get("file_name", ""),
                    "url": a.get("url", ""),
                    "content_type": a.get("file_type") or "",
                })

        # 4. Write extracts
        _put(f"{prefix}/messages.jsonl",
             "\n".join(json.dumps(m) for m in messages_flat),
             "application/x-ndjson")
        if all_tool_uses or all_tool_results:
            lines = [json.dumps({"kind": "tool_use", **x}) for x in all_tool_uses]
            lines += [json.dumps({"kind": "tool_result", **x}) for x in all_tool_results]
            _put(f"{prefix}/tool-calls.jsonl", "\n".join(lines), "application/x-ndjson")
        if all_thinking:
            _put(f"{prefix}/thinking.jsonl",
                 "\n".join(json.dumps(t) for t in all_thinking),
                 "application/x-ndjson")

        # 5. Per-artifact + per-code-block objects
        for a in all_artifacts:
            ext = extension_for_artifact(a)
            safe = re.sub(r"[^a-zA-Z0-9_-]", "_", a["identifier"])[:60]
            key = f"{prefix}/artifacts/{safe}.{ext}"
            _put(key, a["content"], "text/plain; charset=utf-8")
            a["minio_key"] = key
        for i, b in enumerate(all_code_blocks):
            ext = b["language"] if b["language"] in CODE_EXTENSIONS else "txt"
            key = f"{prefix}/code-blocks/{i:04d}_{b['language']}.{ext}"
            _put(key, b["content"], "text/plain; charset=utf-8")
            b["minio_key"] = key

        # 6. Attachment metadata -> backfill_attachments rows (actual bytes come later)
        all_attachments_meta = []
        for ref in file_refs:
            all_attachments_meta.append(("file", ref))
        for ref in image_refs:
            all_attachments_meta.append(("image", ref))

        with _pg() as conn, conn.cursor() as cur:
            for kind, ref in all_attachments_meta:
                att_id = hashlib.sha256(
                    f"{conv_uuid}|{kind}|{ref.get('url', '')}|{ref.get('message_index', -1)}".encode()
                ).hexdigest()[:24]
                cur.execute(
                    """INSERT INTO backfill_attachments
                       (id, conv_uuid, kind, source_url, file_name, content_type, size_bytes, is_code_like)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (id) DO NOTHING""",
                    (att_id, conv_uuid, kind, ref.get("url", ""),
                     ref.get("file_name", ""), ref.get("content_type", ""),
                     ref.get("file_size", 0),
                     is_code_like(ref.get("file_name", ""), ref.get("content_type", ""))),
                )
            conn.commit()

        if file_refs or image_refs:
            _put(f"{prefix}/attachments.json",
                 json.dumps({"files": file_refs, "images": image_refs}, indent=2),
                 "application/json")

        # 7. Manifest
        manifest = {
            "uuid": conv_uuid,
            "name": meta["name"],
            "artifact_count": len(all_artifacts),
            "code_block_count": len(all_code_blocks),
            "tool_call_count": len(all_tool_uses),
            "thinking_block_count": len(all_thinking),
            "file_ref_count": len(file_refs),
            "image_ref_count": len(image_refs),
            "attachment_count": len(file_refs) + len(image_refs),
            "artifacts": [{k: v for k, v in a.items() if k != "content"} for a in all_artifacts],
            "code_blocks": [{k: v for k, v in b.items() if k != "content"} for b in all_code_blocks],
        }
        _put(f"{prefix}/manifest.json", json.dumps(manifest, indent=2), "application/json")

        # 8. Update state
        with _pg() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO backfill_state
                   (conv_uuid, org_uuid, status, harvested_at, message_count,
                    artifact_count, code_block_count, tool_call_count, thinking_count,
                    attachment_total, payload_bytes)
                   VALUES (%s, %s, 'harvested', now(), %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (conv_uuid) DO UPDATE SET
                       status = 'harvested', harvested_at = now(), error = NULL,
                       message_count = EXCLUDED.message_count,
                       artifact_count = EXCLUDED.artifact_count,
                       code_block_count = EXCLUDED.code_block_count,
                       tool_call_count = EXCLUDED.tool_call_count,
                       thinking_count = EXCLUDED.thinking_count,
                       attachment_total = EXCLUDED.attachment_total,
                       payload_bytes = EXCLUDED.payload_bytes""",
                (conv_uuid, body.org_uuid, meta["message_count"],
                 len(all_artifacts), len(all_code_blocks),
                 len(all_tool_uses), len(all_thinking),
                 len(file_refs) + len(image_refs), len(payload_bytes)),
            )
            conn.commit()

        return {
            "status": "harvested",
            "conv_uuid": conv_uuid,
            "artifacts": len(all_artifacts),
            "code_blocks": len(all_code_blocks),
            "tool_calls": len(all_tool_uses),
            "thinking_blocks": len(all_thinking),
            "attachment_refs": len(file_refs) + len(image_refs),
            "payload_bytes": len(payload_bytes),
        }

    except Exception as e:
        logger.exception("ingest failed for %s", conv_uuid)
        with _pg() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO backfill_state (conv_uuid, org_uuid, status, error)
                   VALUES (%s, %s, 'failed', %s)
                   ON CONFLICT (conv_uuid) DO UPDATE SET status='failed', error=EXCLUDED.error""",
                (conv_uuid, body.org_uuid, str(e)[:500]),
            )
            conn.commit()
        raise HTTPException(500, f"ingest failed: {e}")


@router.post("/attachment")
async def ingest_attachment(body: AttachmentRequest, _: dict = Depends(require_auth)) -> dict:
    """Store one attachment blob. Size-capped for non-code types."""
    att_id = hashlib.sha256(
        f"{body.conv_uuid}|{body.kind}|{body.source_url}|{body.message_index}".encode()
    ).hexdigest()[:24]

    code_like = is_code_like(body.file_name, body.content_type)

    if body.data_b64 is None:
        with _pg() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO backfill_attachments
                   (id, conv_uuid, kind, source_url, file_name, content_type, is_code_like, status)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
                   ON CONFLICT (id) DO NOTHING""",
                (att_id, body.conv_uuid, body.kind, body.source_url,
                 body.file_name, body.content_type, code_like),
            )
            conn.commit()
        return {"status": "queued", "id": att_id}

    try:
        data = base64.b64decode(body.data_b64)
    except Exception as e:
        raise HTTPException(400, f"invalid base64: {e}")

    size = len(data)
    sha = hashlib.sha256(data).hexdigest()

    # Size cap for non-code uploads
    if not code_like and size > ATTACHMENT_SIZE_CAP_BYTES:
        with _pg() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO backfill_attachments
                   (id, conv_uuid, kind, source_url, file_name, content_type,
                    size_bytes, is_code_like, status, sha256, error)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'skipped_oversize', %s, %s)
                   ON CONFLICT (id) DO UPDATE SET
                       size_bytes = EXCLUDED.size_bytes,
                       status = 'skipped_oversize',
                       sha256 = EXCLUDED.sha256,
                       error = EXCLUDED.error""",
                (att_id, body.conv_uuid, body.kind, body.source_url,
                 body.file_name, body.content_type, size, code_like, sha,
                 f"over {ATTACHMENT_SIZE_CAP_BYTES} byte cap for non-code"),
            )
            conn.commit()
        return {"status": "skipped_oversize", "size": size, "cap": ATTACHMENT_SIZE_CAP_BYTES}

    # Write to MinIO
    safe_fn = re.sub(r"[^a-zA-Z0-9._-]", "_", body.file_name or att_id)[:120]
    key = f"conversations/{body.conv_uuid}/attachments/{att_id}_{safe_fn}"
    _put(key, data, body.content_type or "application/octet-stream")

    with _pg() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO backfill_attachments
               (id, conv_uuid, kind, source_url, file_name, content_type,
                size_bytes, is_code_like, status, minio_key, sha256, stored_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'stored', %s, %s, now())
               ON CONFLICT (id) DO UPDATE SET
                   size_bytes = EXCLUDED.size_bytes,
                   status = 'stored',
                   minio_key = EXCLUDED.minio_key,
                   sha256 = EXCLUDED.sha256,
                   stored_at = now(),
                   error = NULL""",
            (att_id, body.conv_uuid, body.kind, body.source_url,
             body.file_name, body.content_type, size, code_like, key, sha),
        )
        cur.execute(
            """UPDATE backfill_state SET attachment_done = attachment_done + 1
               WHERE conv_uuid = %s""",
            (body.conv_uuid,),
        )
        conn.commit()

    return {"status": "stored", "id": att_id, "minio_key": key, "size": size, "sha256": sha}
