"""Compression Proxy Router — Transparent API Proxy with Token Savings

Sits between users and LLM providers (Anthropic, OpenAI).
Compresses input, injects compression spec, expands output.
User changes ONE setting: their API base URL.

Supported endpoints:
  POST /proxy/v1/messages        — Anthropic Messages API (drop-in)
  POST /proxy/v1/chat/completions — OpenAI Chat Completions API (drop-in)

Flow:
  1. Receive user request (normal API call)
  2. Compress user messages (programmatic, free)
  3. Inject compression spec into system prompt (~137 tokens, cached)
  4. Forward to real provider API
  5. Receive compressed response from LLM
  6. Expand to natural English (programmatic, free)
  7. Return expanded response to user
  8. Log token savings metrics

The user sees normal readable responses.
The LLM generates compressed tokens.
The bill is smaller.
"""
import json
import logging
import time
import os
from typing import Optional, Dict, Any, List

import httpx
from fastapi import APIRouter, Request, Response
from fastapi.responses import StreamingResponse, JSONResponse

from services.language_compression import get_language_compression

log = logging.getLogger("helix.proxy")

proxy_router = APIRouter(prefix="/proxy")

# Provider base URLs
ANTHROPIC_URL = "https://api.anthropic.com"
OPENAI_URL = "https://api.openai.com"

# HTTP client (persistent connection pool)
_http: Optional[httpx.AsyncClient] = None

def _get_http() -> httpx.AsyncClient:
    global _http
    if _http is None or _http.is_closed:
        _http = httpx.AsyncClient(timeout=120)
    return _http


# ============================================================
# ANTHROPIC MESSAGES API PROXY
# ============================================================

@proxy_router.post("/v1/messages")
async def proxy_anthropic_messages(request: Request):
    """Drop-in replacement for api.anthropic.com/v1/messages.

    User sends exact same request they'd send to Anthropic.
    We compress, forward, expand, return.
    """
    start = time.time()
    body = await request.json()
    headers = dict(request.headers)

    # Extract API key from header (user provides their own)
    api_key = headers.get("x-api-key", "")
    anthropic_version = headers.get("anthropic-version", "2023-06-01")

    if not api_key:
        return JSONResponse(
            status_code=401,
            content={"error": {"message": "Missing x-api-key header"}}
        )

    svc = get_language_compression()
    spec_data = svc.get_spec()
    metrics = {"input_compressed": 0, "output_expanded": 0}

    # Step 1: Inject compression spec into system prompt
    system = body.get("system", "")
    if isinstance(system, str):
        body["system"] = system + "\n\n" + spec_data["spec"]
    elif isinstance(system, list):
        # System can be list of content blocks
        body["system"] = system + [{"type": "text", "text": "\n\n" + spec_data["spec"]}]

    # Step 2: Compress user messages
    messages = body.get("messages", [])
    for msg in messages:
        if msg.get("role") == "user":
            content = msg.get("content", "")
            if isinstance(content, str) and len(content) > 20:
                result = svc.compress(content)
                msg["content"] = result["compressed"]
                metrics["input_compressed"] += result["tokens_saved"]

    # Step 3: Check if streaming
    is_stream = body.get("stream", False)

    # Step 4: Forward to Anthropic
    forward_headers = {
        "x-api-key": api_key,
        "anthropic-version": anthropic_version,
        "content-type": "application/json",
    }

    http = _get_http()

    if is_stream:
        # Streaming: proxy chunks, expand text events
        return StreamingResponse(
            _stream_anthropic(http, body, forward_headers, svc, metrics),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Compression-Input-Saved": str(metrics["input_compressed"]),
            }
        )
    else:
        # Non-streaming: get full response, expand, return
        try:
            resp = await http.post(
                f"{ANTHROPIC_URL}/v1/messages",
                json=body,
                headers=forward_headers,
            )

            if resp.status_code != 200:
                return JSONResponse(
                    status_code=resp.status_code,
                    content=resp.json()
                )

            data = resp.json()

            # Step 5: Expand assistant content
            for block in data.get("content", []):
                if block.get("type") == "text":
                    original = block["text"]
                    expanded = svc.expand(original)
                    block["text"] = expanded["expanded"]
                    metrics["output_expanded"] += expanded.get("tokens_restored", 0)

            # Add compression metrics to response
            duration = time.time() - start
            data["_compression"] = {
                "input_tokens_saved": metrics["input_compressed"],
                "output_tokens_expanded": metrics["output_expanded"],
                "proxy_latency_ms": round(duration * 1000),
                "spec_version": "2.0",
            }

            return JSONResponse(content=data)

        except httpx.TimeoutException:
            return JSONResponse(
                status_code=504,
                content={"error": {"message": "Upstream timeout"}}
            )
        except Exception as e:
            log.error(f"Proxy error: {e}")
            return JSONResponse(
                status_code=502,
                content={"error": {"message": f"Proxy error: {str(e)}"}}
            )


async def _stream_anthropic(http, body, headers, svc, metrics):
    """Stream Anthropic response, expanding text deltas on the fly."""
    try:
        async with http.stream(
            "POST", f"{ANTHROPIC_URL}/v1/messages",
            json=body, headers=headers,
        ) as resp:
            text_buffer = ""
            async for line in resp.aiter_lines():
                if not line.startswith("data: "):
                    yield line + "\n"
                    continue

                data_str = line[6:]
                if data_str.strip() == "[DONE]":
                    # Expand any remaining buffered text
                    if text_buffer:
                        expanded = svc.expand(text_buffer)
                        # Emit as final delta
                        yield f"data: {json.dumps({'type':'content_block_delta','delta':{'type':'text_delta','text':expanded['expanded']}})}\n"
                        text_buffer = ""
                    yield line + "\n"
                    continue

                try:
                    event = json.loads(data_str)

                    # Accumulate text deltas, expand on sentence boundaries
                    if event.get("type") == "content_block_delta":
                        delta = event.get("delta", {})
                        if delta.get("type") == "text_delta":
                            text = delta.get("text", "")
                            text_buffer += text

                            # Check for sentence boundary
                            if any(text_buffer.rstrip().endswith(c) for c in '.!?\n'):
                                expanded = svc.expand(text_buffer)
                                delta["text"] = expanded["expanded"]
                                event["delta"] = delta
                                text_buffer = ""
                                yield f"data: {json.dumps(event)}\n"
                                continue

                            # Don't emit yet — still buffering
                            continue

                    # Pass through non-text events unchanged
                    yield f"data: {json.dumps(event)}\n"

                except json.JSONDecodeError:
                    yield line + "\n"

    except Exception as e:
        log.error(f"Stream proxy error: {e}")
        yield f"data: {json.dumps({'type':'error','error':{'message':str(e)}})}\n"


# ============================================================
# OPENAI CHAT COMPLETIONS PROXY
# ============================================================

@proxy_router.post("/v1/chat/completions")
async def proxy_openai_chat(request: Request):
    """Drop-in replacement for api.openai.com/v1/chat/completions.

    Also works with any OpenAI-compatible API (Ollama, vLLM, etc).
    User sends exact same request, we compress/expand transparently.
    """
    start = time.time()
    body = await request.json()
    headers = dict(request.headers)

    api_key = headers.get("authorization", "")
    if not api_key:
        return JSONResponse(
            status_code=401,
            content={"error": {"message": "Missing Authorization header"}}
        )

    svc = get_language_compression()
    spec_data = svc.get_spec()
    metrics = {"input_compressed": 0, "output_expanded": 0}

    # Inject compression spec into system message
    messages = body.get("messages", [])
    has_system = any(m.get("role") == "system" for m in messages)

    if has_system:
        for msg in messages:
            if msg.get("role") == "system":
                msg["content"] = msg.get("content", "") + "\n\n" + spec_data["spec"]
                break
    else:
        messages.insert(0, {"role": "system", "content": spec_data["spec"]})

    # Compress user messages
    for msg in messages:
        if msg.get("role") == "user":
            content = msg.get("content", "")
            if isinstance(content, str) and len(content) > 20:
                result = svc.compress(content)
                msg["content"] = result["compressed"]
                metrics["input_compressed"] += result["tokens_saved"]

    # Determine target URL (allow custom base via header)
    base_url = headers.get("x-proxy-target", OPENAI_URL).rstrip("/")

    forward_headers = {
        "authorization": api_key,
        "content-type": "application/json",
    }

    is_stream = body.get("stream", False)
    http = _get_http()

    if is_stream:
        return StreamingResponse(
            _stream_openai(http, body, forward_headers, base_url, svc, metrics),
            media_type="text/event-stream",
        )
    else:
        try:
            resp = await http.post(
                f"{base_url}/v1/chat/completions",
                json=body, headers=forward_headers,
            )

            if resp.status_code != 200:
                return JSONResponse(status_code=resp.status_code, content=resp.json())

            data = resp.json()

            # Expand assistant content
            for choice in data.get("choices", []):
                msg = choice.get("message", {})
                if msg.get("role") == "assistant" and msg.get("content"):
                    expanded = svc.expand(msg["content"])
                    msg["content"] = expanded["expanded"]
                    metrics["output_expanded"] += expanded.get("tokens_restored", 0)

            duration = time.time() - start
            data["_compression"] = {
                "input_tokens_saved": metrics["input_compressed"],
                "output_tokens_expanded": metrics["output_expanded"],
                "proxy_latency_ms": round(duration * 1000),
            }

            return JSONResponse(content=data)

        except httpx.TimeoutException:
            return JSONResponse(status_code=504, content={"error": {"message": "Upstream timeout"}})
        except Exception as e:
            log.error(f"OpenAI proxy error: {e}")
            return JSONResponse(status_code=502, content={"error": {"message": str(e)}})


async def _stream_openai(http, body, headers, base_url, svc, metrics):
    """Stream OpenAI response, expanding text deltas."""
    try:
        async with http.stream(
            "POST", f"{base_url}/v1/chat/completions",
            json=body, headers=headers,
        ) as resp:
            text_buffer = ""
            async for line in resp.aiter_lines():
                if not line.startswith("data: "):
                    yield line + "\n"
                    continue

                data_str = line[6:]
                if data_str.strip() == "[DONE]":
                    if text_buffer:
                        expanded = svc.expand(text_buffer)
                        yield f"data: {json.dumps({'choices':[{'delta':{'content':expanded['expanded']}}]})}\n"
                    yield line + "\n"
                    continue

                try:
                    event = json.loads(data_str)
                    choices = event.get("choices", [])
                    if choices:
                        delta = choices[0].get("delta", {})
                        content = delta.get("content", "")
                        if content:
                            text_buffer += content
                            if any(text_buffer.rstrip().endswith(c) for c in '.!?\n'):
                                expanded = svc.expand(text_buffer)
                                delta["content"] = expanded["expanded"]
                                choices[0]["delta"] = delta
                                text_buffer = ""
                                yield f"data: {json.dumps(event)}\n"
                                continue
                            continue

                    yield f"data: {json.dumps(event)}\n"
                except json.JSONDecodeError:
                    yield line + "\n"

    except Exception as e:
        log.error(f"OpenAI stream error: {e}")
        yield f"data: {json.dumps({'error': str(e)})}\n"


# ============================================================
# PROXY INFO
# ============================================================

@proxy_router.get("/info")
async def proxy_info():
    """Get proxy configuration and supported providers."""
    svc = get_language_compression()
    spec = svc.get_spec()
    return {
        "status": "active",
        "providers": {
            "anthropic": {
                "endpoint": "/proxy/v1/messages",
                "target": ANTHROPIC_URL,
                "auth_header": "x-api-key",
            },
            "openai": {
                "endpoint": "/proxy/v1/chat/completions",
                "target": OPENAI_URL,
                "auth_header": "Authorization: Bearer ...",
                "custom_target": "X-Proxy-Target header to override base URL",
            },
        },
        "compression": {
            "spec_version": spec["version"],
            "spec_tokens": spec["spec_tokens"],
            "phrase_count": spec["phrase_count"],
            "abbreviation_count": spec["abbreviation_count"],
        },
    }
