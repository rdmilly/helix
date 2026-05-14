"""Transcript Bridge — Accepts transcripts, stores raw, forwards to Helix.

Endpoints:
  POST /api/v1/transcripts — Accept a transcript from MillyExt or cron
  POST /api/v1/transcripts/batch — Accept multiple transcripts
  GET  /api/v1/stats — Stats
  GET  / — Health

Flow:
  1. Receive transcript JSON from extension/cron
  2. Store raw JSON to /app/data/transcripts/ (backup)
  3. Forward text to Helix /api/v1/conversations/ingest (async)
  4. Return result
"""
import json
import logging
import os
import gzip
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any

import httpx
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Transcript Bridge", version="1.0.0")

DATA_DIR = Path("/app/data")
TRANSCRIPT_DIR = DATA_DIR / "transcripts"
LOG_DIR = DATA_DIR / "logs"
TRANSCRIPT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

HELIX_URL = os.getenv("HELIX_URL", "http://helix-cortex:9050")
API_KEY = os.getenv("BRIDGE_API_KEY", "bridge-dev-key")

# Stats tracking
stats = {
    "received": 0,
    "forwarded": 0,
    "forward_failed": 0,
    "stored": 0,
    "total_bytes": 0,
    "started_at": datetime.now(timezone.utc).isoformat(),
}


class TranscriptPayload(BaseModel):
    """Transcript from MillyExt or cron."""
    conversation_id: str = Field("", description="Claude conversation ID")
    name: str = Field("", description="Conversation title/name")
    messages: Optional[List[Dict[str, Any]]] = Field(None, description="Structured messages")
    text: Optional[str] = Field(None, description="Raw text transcript")
    source: str = Field("millyext", description="Source identifier")
    model: Optional[str] = Field(None, description="Model used")
    message_count: Optional[int] = Field(None, description="Number of messages")
    created_at: Optional[str] = Field(None, description="When conversation started")
    updated_at: Optional[str] = Field(None, description="Last update time")


class BatchPayload(BaseModel):
    transcripts: List[TranscriptPayload]


def _extract_text(payload: TranscriptPayload) -> str:
    """Extract searchable text from transcript payload."""
    if payload.text:
        return payload.text

    if payload.messages:
        parts = []
        for msg in payload.messages:
            role = msg.get("role", "unknown")
            # Handle different content formats
            content = msg.get("content", "")
            if isinstance(content, list):
                # Multi-part content (text + tool_use etc)
                text_parts = []
                for block in content:
                    if isinstance(block, dict):
                        if block.get("type") == "text":
                            text_parts.append(block.get("text", ""))
                        elif block.get("type") == "tool_use":
                            text_parts.append(f"[tool: {block.get('name', '')}]")
                        elif block.get("type") == "tool_result":
                            text_parts.append(f"[tool_result]")
                    elif isinstance(block, str):
                        text_parts.append(block)
                content = "\n".join(text_parts)
            elif not isinstance(content, str):
                content = str(content)

            label = "Human" if role in ("user", "human") else "Assistant"
            parts.append(f"{label}: {content}")
        return "\n\n".join(parts)

    return ""


def _store_raw(payload: TranscriptPayload) -> str:
    """Store raw transcript JSON to disk (gzipped)."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_name = (payload.name or "unnamed")[:60].replace("/", "_").replace(" ", "_")
    filename = f"{ts}_{safe_name}.json.gz"
    filepath = TRANSCRIPT_DIR / filename

    raw = payload.dict()
    data = json.dumps(raw, default=str).encode()
    with gzip.open(filepath, "wb") as f:
        f.write(data)

    stats["stored"] += 1
    stats["total_bytes"] += len(data)

    return str(filepath)


async def _forward_to_helix(payload: TranscriptPayload, text: str) -> Dict:
    """Forward extracted text to Helix conversation ingest."""
    session_id = payload.conversation_id or hashlib.sha256(
        text[:500].encode()
    ).hexdigest()[:16]

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{HELIX_URL}/api/v1/conversations/ingest",
                json={
                    "text": text,
                    "session_id": session_id,
                    "source": payload.source,
                    "timestamp": payload.updated_at or payload.created_at or datetime.now(timezone.utc).isoformat(),
                    "metadata": {
                        "name": payload.name,
                        "model": payload.model or "",
                        "message_count": str(payload.message_count or 0),
                    },
                    "scan_code": True,
                },
            )
            if resp.status_code == 200:
                stats["forwarded"] += 1
                return resp.json()
            else:
                stats["forward_failed"] += 1
                logger.error(f"Helix forward failed: {resp.status_code} {resp.text[:200]}")
                return {"status": "forward_failed", "http_status": resp.status_code}
    except Exception as e:
        stats["forward_failed"] += 1
        logger.error(f"Helix forward error: {e}")
        return {"status": "forward_error", "error": str(e)}


# === Routes ===

@app.post("/api/v1/transcripts")
async def ingest_transcript(
    payload: TranscriptPayload,
    x_api_key: str = Header(None),
):
    """Accept a transcript, store raw, forward to Helix."""
    if x_api_key and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    stats["received"] += 1

    # Extract text
    text = _extract_text(payload)
    if not text or len(text) < 20:
        return {"status": "skipped", "reason": "too short", "chars": len(text)}

    # Store raw backup
    stored_path = _store_raw(payload)

    # Forward to Helix
    helix_result = await _forward_to_helix(payload, text)

    # Log
    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "conversation_id": payload.conversation_id,
        "name": payload.name,
        "message_count": payload.message_count,
        "size_bytes": len(text),
        "stored_at": stored_path,
        "helix_status": helix_result.get("status", "unknown"),
        "chunks": helix_result.get("chunks", 0),
    }
    log_file = LOG_DIR / f"ingest_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.jsonl"
    with open(log_file, "a") as f:
        f.write(json.dumps(log_entry) + "\n")

    return {
        "status": "processed",
        "stored": stored_path,
        "helix": helix_result,
        "text_chars": len(text),
    }


@app.post("/api/v1/transcripts/batch")
async def ingest_batch(payload: BatchPayload, x_api_key: str = Header(None)):
    """Batch ingest multiple transcripts."""
    if x_api_key and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    results = []
    for t in payload.transcripts:
        stats["received"] += 1
        text = _extract_text(t)
        if not text or len(text) < 20:
            results.append({"status": "skipped", "reason": "too short"})
            continue
        stored_path = _store_raw(t)
        helix_result = await _forward_to_helix(t, text)
        results.append({
            "status": "processed",
            "conversation_id": t.conversation_id,
            "chunks": helix_result.get("chunks", 0),
        })

    return {
        "status": "batch_complete",
        "total": len(results),
        "processed": sum(1 for r in results if r["status"] == "processed"),
        "results": results,
    }


@app.get("/api/v1/stats")
async def get_stats():
    """Bridge statistics."""
    transcript_count = len(list(TRANSCRIPT_DIR.glob("*.json.gz")))
    total_size = sum(f.stat().st_size for f in TRANSCRIPT_DIR.glob("*.json.gz"))
    return {
        **stats,
        "transcripts_on_disk": transcript_count,
        "disk_size_mb": round(total_size / 1024 / 1024, 2),
    }


@app.get("/")
async def root():
    return {
        "service": "Transcript Bridge",
        "version": "1.0.0",
        "status": "healthy",
        "helix_target": HELIX_URL,
        "received": stats["received"],
        "forwarded": stats["forwarded"],
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8099)
