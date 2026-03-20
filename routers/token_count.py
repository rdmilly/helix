"""
Token Count Router — exact token counting via tiktoken
Used by Membrane compression.js to get real before/after counts.
"""
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
import logging

log = logging.getLogger("helix.token_count")

try:
    import tiktoken
    _enc = tiktoken.get_encoding("cl100k_base")
    TIKTOKEN_AVAILABLE = True
except Exception as e:
    log.warning(f"tiktoken not available: {e}")
    TIKTOKEN_AVAILABLE = False
    _enc = None

token_count_router = APIRouter(prefix="/api/v1/tokens", tags=["tokens"])


class CountRequest(BaseModel):
    raw: str                    # uncompressed text
    compressed: Optional[str] = None  # compressed text (optional)


class CountResponse(BaseModel):
    raw_tokens: int
    compressed_tokens: Optional[int] = None
    saved_tokens: Optional[int] = None
    savings_pct: Optional[float] = None
    method: str  # 'tiktoken' or 'estimate'


def count_tokens(text: str) -> int:
    if TIKTOKEN_AVAILABLE and _enc:
        return len(_enc.encode(text))
    # fallback: 4 chars per token
    return max(1, len(text) // 4)


@token_count_router.post("/count", response_model=CountResponse)
async def count_tokens_endpoint(req: CountRequest):
    """Count tokens in raw and compressed text. Returns exact counts via tiktoken."""
    method = "tiktoken" if TIKTOKEN_AVAILABLE else "estimate"
    raw_tokens = count_tokens(req.raw)

    if req.compressed is not None:
        compressed_tokens = count_tokens(req.compressed)
        saved = max(0, raw_tokens - compressed_tokens)
        pct = round(saved / raw_tokens * 100, 1) if raw_tokens > 0 else 0.0
        return CountResponse(
            raw_tokens=raw_tokens,
            compressed_tokens=compressed_tokens,
            saved_tokens=saved,
            savings_pct=pct,
            method=method,
        )

    return CountResponse(raw_tokens=raw_tokens, method=method)


@token_count_router.get("/health")
async def token_count_health():
    return {"status": "ok", "tiktoken": TIKTOKEN_AVAILABLE, "encoding": "cl100k_base"}
