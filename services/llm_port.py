"""LLMPort — Phase 7: Provider-agnostic LLM interface.

Single async `chat()` call routes to the configured provider.
Provider selected via LLM_PROVIDER env var (default: openrouter).

Supported providers:
  openrouter  OpenAI-compat via openrouter.ai  (current default)
  anthropic   Direct Anthropic Messages API
  ollama      Local Ollama OpenAI-compat endpoint

All callers (haiku.py) use only:
    from services.llm_port import get_llm_port
    text = await get_llm_port().chat(messages, model, max_tokens)
"""
import logging
import os
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------
# Config (all overridable via env)
# ---------------------------------------------------------------
LLM_PROVIDER   = os.getenv("LLM_PROVIDER",   "openrouter")
LLM_MODEL      = os.getenv("LLM_MODEL",       "")  # empty = use provider default
LLM_TIMEOUT    = int(os.getenv("LLM_TIMEOUT", "30"))

# OpenRouter
OPENROUTER_KEY     = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE    = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
OPENROUTER_DEFAULT = os.getenv("HAIKU_MODEL", "anthropic/claude-haiku-4-5")

# Anthropic direct
ANTHROPIC_KEY     = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_BASE    = "https://api.anthropic.com"
ANTHROPIC_DEFAULT = "claude-haiku-4-5-20251001"
ANTHROPIC_VERSION = "2023-06-01"

# Ollama
OLLAMA_BASE    = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_DEFAULT = os.getenv("OLLAMA_MODEL",    "llama3")


class LLMPort:
    """
    Provider-agnostic LLM interface.
    Instantiated once as a singleton; client reused across calls.
    """

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None
        self.provider = LLM_PROVIDER.lower()
        # Resolved model: LLM_MODEL env > provider default
        self.default_model = (
            LLM_MODEL or {
                "openrouter": OPENROUTER_DEFAULT,
                "anthropic":  ANTHROPIC_DEFAULT,
                "ollama":     OLLAMA_DEFAULT,
            }.get(self.provider, OPENROUTER_DEFAULT)
        )
        logger.info(f"LLMPort: provider={self.provider} model={self.default_model}")

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            headers = self._build_headers()
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(LLM_TIMEOUT),
                headers=headers,
            )
        return self._client

    def _build_headers(self) -> Dict[str, str]:
        if self.provider == "anthropic":
            return {
                "x-api-key":         ANTHROPIC_KEY,
                "anthropic-version": ANTHROPIC_VERSION,
                "content-type":      "application/json",
            }
        if self.provider == "ollama":
            return {"Content-Type": "application/json"}
        # openrouter (default)
        return {
            "Authorization": f"Bearer {OPENROUTER_KEY}",
            "Content-Type":  "application/json",
            "HTTP-Referer":  "https://helix.millyweb.com",
            "X-Title":       "Helix Cortex",
        }

    def _build_request(self, messages: List[Dict], model: str, max_tokens: int) -> tuple[str, Dict]:
        """Return (url, json_body) for the configured provider."""
        if self.provider == "anthropic":
            # Separate system message from user turns
            system = ""
            user_msgs = []
            for m in messages:
                if m["role"] == "system":
                    system = m["content"]
                else:
                    user_msgs.append(m)
            body: Dict[str, Any] = {
                "model":      model,
                "max_tokens": max_tokens,
                "messages":   user_msgs,
            }
            if system:
                body["system"] = system
            return f"{ANTHROPIC_BASE}/v1/messages", body

        if self.provider == "ollama":
            return f"{OLLAMA_BASE}/v1/chat/completions", {
                "model":      model,
                "max_tokens": max_tokens,
                "messages":   messages,
                "stream":     False,
            }

        # openrouter (default) — OpenAI-compat
        return f"{OPENROUTER_BASE}/chat/completions", {
            "model":      model,
            "max_tokens": max_tokens,
            "messages":   messages,
        }

    def _parse_response(self, data: Dict) -> str:
        """Extract text from provider response dict."""
        if self.provider == "anthropic":
            # Anthropic Messages API: {content: [{type: text, text: ...}]}
            for block in data.get("content", []):
                if block.get("type") == "text":
                    return block.get("text", "")
            return ""
        # OpenAI-compat (openrouter, ollama)
        return data.get("choices", [{}])[0].get("message", {}).get("content", "")

    async def chat(
        self,
        messages: List[Dict[str, str]],
        model:      Optional[str] = None,
        max_tokens: int = 1024,
    ) -> Optional[str]:
        """
        Send a chat request to the configured provider.
        Returns the response text, or None on error.
        """
        resolved_model = model or self.default_model
        url, body = self._build_request(messages, resolved_model, max_tokens)

        try:
            client = await self._get_client()
            resp = await client.post(url, json=body)

            if resp.status_code == 200:
                return self._parse_response(resp.json())

            if resp.status_code == 429:
                logger.warning(f"LLMPort rate limited by {self.provider}")
                return None

            logger.error(f"LLMPort {self.provider} error {resp.status_code}: {resp.text[:200]}")
            return None

        except httpx.TimeoutException:
            logger.error(f"LLMPort {self.provider} timeout")
            return None
        except Exception as e:
            logger.error(f"LLMPort {self.provider} exception: {e}")
            return None

    async def health_check(self) -> bool:
        """Ping the provider with a minimal request."""
        result = await self.chat(
            messages=[{"role": "user", "content": "ping"}],
            max_tokens=5,
        )
        return result is not None

    async def close(self):
        if self._client:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None

    def describe(self) -> Dict[str, str]:
        return {"provider": self.provider, "model": self.default_model}


# Global singleton
_llm_port: Optional[LLMPort] = None


def get_llm_port() -> LLMPort:
    global _llm_port
    if _llm_port is None:
        _llm_port = LLMPort()
    return _llm_port
