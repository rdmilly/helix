"""Haiku Service - Claude Haiku API Client

Phase 2: Real LLM calls for summarization, classification,
entity extraction, and decision extraction.

Uses httpx for async HTTP to Anthropic API directly.
Circuit breaker protects against cascading failures.
"""
import json
import logging
import time
from typing import Optional, Dict, Any, List

import httpx

from config import OPENROUTER_API_KEY, OPENROUTER_BASE_URL, HAIKU_MODEL, HAIKU_MAX_RETRIES, HAIKU_TIMEOUT
from services.llm_port import get_llm_port
from services.parser import extract_json, extract_json_or_default

logger = logging.getLogger(__name__)

# OpenRouter OpenAI-compat endpoint
OPENROUTER_CHAT_URL = f"{OPENROUTER_BASE_URL}/chat/completions"


class CircuitBreaker:
    """Simple circuit breaker for external service calls."""
    
    def __init__(self, threshold: int = 5, timeout: int = 60):
        self.threshold = threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.is_open = False
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.threshold:
            self.is_open = True
            logger.error(f"Circuit breaker opened after {self.failure_count} failures")
    
    def record_success(self):
        self.failure_count = 0
        if self.is_open:
            self.is_open = False
            logger.info("Circuit breaker closed")
    
    def can_execute(self) -> bool:
        if not self.is_open:
            return True
        # Check if timeout has passed (half-open state)
        if time.time() - self.last_failure_time > self.timeout:
            logger.info("Circuit breaker half-open, allowing probe request")
            return True
        return False


class HaikuService:
    """Claude Haiku API client with circuit breaker."""
    
    def __init__(self):
        self.api_key = OPENROUTER_API_KEY
        self.model = HAIKU_MODEL
        self.max_retries = HAIKU_MAX_RETRIES
        self.timeout = HAIKU_TIMEOUT
        self.circuit_breaker = CircuitBreaker()
        self._client: Optional[httpx.AsyncClient] = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the async HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                headers={
                    "Authorization": f"Bearer {self.api_key or ''}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://helix.millyweb.com",
                    "X-Title": "Helix Cortex",
                }
            )
        return self._client
    
    async def _call_api(self, system: str, user_message: str, max_tokens: int = 1024) -> Optional[str]:
        """Make a single API call via LLMPort (provider-agnostic)."""
        if not self.circuit_breaker.can_execute():
            logger.warning("Haiku circuit breaker open, skipping API call")
            return None

        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": user_message})

        for attempt in range(self.max_retries):
            try:
                text = await get_llm_port().chat(
                    messages=messages,
                    model=self.model,
                    max_tokens=max_tokens,
                )
                if text is not None:
                    self.circuit_breaker.record_success()
                    return text
                # None return = provider error, apply circuit breaker and retry
                self.circuit_breaker.record_failure()
                if attempt < self.max_retries - 1:
                    import asyncio
                    await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Haiku _call_api attempt {attempt+1} error: {e}")
                self.circuit_breaker.record_failure()

        logger.error(f"Haiku API failed after {self.max_retries} retries")
        return None
    
    # ============================================================
    # Public API Methods
    # ============================================================
    
    async def summarize_session(self, messages: list, context: Optional[str] = None) -> str:
        """Summarize a conversation session into a concise paragraph."""
        system = (
            "You are a concise session summarizer. Given conversation messages, "
            "produce a 2-4 sentence summary capturing: what was discussed, "
            "what was decided, and what was built or changed. "
            "Focus on technical outcomes and decisions. "
            "Respond ONLY with the summary text, no preamble."
        )
        
        # Format messages for the prompt
        formatted = []
        for msg in messages[:50]:  # Cap at 50 messages to stay within limits
            role = msg.get("role", "unknown")
            content = msg.get("content", "")
            if isinstance(content, str):
                formatted.append(f"[{role}]: {content[:500]}")
        
        user_msg = "\n".join(formatted)
        if context:
            user_msg = f"Context: {context}\n\nMessages:\n{user_msg}"
        
        result = await self._call_api(system, user_msg, max_tokens=512)
        return result or "Summary unavailable"
    
    async def classify_content(self, content: str) -> Dict[str, Any]:
        """Classify content type: CODE, ACTIONS, TEXT, or CHANGES.
        
        Returns dict with type, confidence, and reasoning.
        """
        system = (
            "You are a content classifier. Classify the given content into exactly one category:\n"
            "- CODE: Source code, functions, classes, scripts\n"
            "- ACTIONS: Tool calls, file operations, shell commands, API calls\n"
            "- TEXT: Natural language discussion, documentation, explanations\n"
            "- CHANGES: Git commits, config changes, infrastructure modifications\n\n"
            "Respond ONLY with valid JSON: {\"type\": \"CODE|ACTIONS|TEXT|CHANGES\", \"confidence\": 0.0-1.0, \"reasoning\": \"brief explanation\"}"
        )
        
        result = await self._call_api(system, content[:4000], max_tokens=256)
        
        if result:
            parsed = extract_json(result)
            if parsed and isinstance(parsed, dict) and parsed.get("type") in ("CODE", "ACTIONS", "TEXT", "CHANGES"):
                return parsed
            logger.warning(f"Classification parse failed or invalid type: {result[:100]}")
        
        # Fallback: heuristic classification
        return self._heuristic_classify(content)
    
    async def extract_entities(self, text: str) -> Dict[str, Any]:
        """Extract entities: people, projects, services, technologies."""
        system = (
            "You are an entity extractor for software development sessions. "
            "Extract named entities from the text into these categories:\n"
            "- people: Names of people mentioned\n"
            "- projects: Software project names\n"
            "- services: Infrastructure services, APIs, tools\n"
            "- technologies: Languages, frameworks, libraries\n\n"
            "Respond ONLY with valid JSON: {\"people\": [], \"projects\": [], \"services\": [], \"technologies\": []}"
        )
        
        result = await self._call_api(system, text[:4000], max_tokens=512)
        
        if result:
            parsed = extract_json(result)
            if parsed and isinstance(parsed, dict):
                return {
                    "people": parsed.get("people", []),
                    "projects": parsed.get("projects", []),
                    "services": parsed.get("services", []),
                    "technologies": parsed.get("technologies", []),
                }
            logger.warning(f"Entity extraction parse failed: {result[:100]}")
        
        return {"people": [], "projects": [], "services": [], "technologies": []}
    
    async def extract_decisions(self, text: str) -> List[Dict[str, Any]]:
        """Extract decisions and commitments from session text."""
        system = (
            "You are a decision extractor for software development sessions. "
            "Identify specific decisions, commitments, and action items from the text.\n\n"
            "Respond ONLY with a valid JSON array of decisions:\n"
            "[{\"decision\": \"what was decided\", \"type\": \"architecture|deployment|config|process|design\", "
            "\"confidence\": 0.0-1.0}]"
        )
        
        result = await self._call_api(system, text[:4000], max_tokens=1024)
        
        if result:
            parsed = extract_json(result)
            if parsed and isinstance(parsed, list):
                return parsed
            logger.warning(f"Decision extraction parse failed: {result[:100]}")
        
        return []
    

    async def extract_intelligence(self, text: str) -> list:
        """Extract structured intelligence using 9-tag taxonomy."""
        system = (
            "You are an intelligence extractor for software design sessions. "
            "Extract ALL significant items using these exact tags:\n\n"
            "DECISION - something explicitly chosen or resolved\n"
            "ASSUMPTION - treated as true but not yet verified (highest priority)\n"
            "CONSTRAINT - external reality limiting options\n"
            "INVARIANT - non-negotiable property the system must always maintain (highest priority)\n"
            "RISK - known risk with no mitigation yet\n"
            "TRADEOFF - accepted cost for a benefit\n"
            "COUPLING - unexpected dependency discovered between components\n"
            "REJECTED - alternative ruled out, always include the reason\n"
            "PATTERN - reusable insight applicable beyond this context\n\n"
            "Return a valid JSON array. Each object has fields: tag, content, component, context, confidence. "
            "Valid tag values: DECISION ASSUMPTION CONSTRAINT INVARIANT RISK TRADEOFF COUPLING REJECTED PATTERN. "
            "Only include items where confidence >= 0.6. "
            "ASSUMPTION and INVARIANT are the highest priority - capture even if implicit. "
            "Skip low-value or obvious items."
        )
        result = await self._call_api(system, text[:5000], max_tokens=2048)
        if result:
            parsed = extract_json(result)
            if parsed and isinstance(parsed, list):
                valid_tags = {
                    "DECISION", "ASSUMPTION", "CONSTRAINT", "INVARIANT",
                    "RISK", "TRADEOFF", "COUPLING", "REJECTED", "PATTERN"
                }
                return [
                    item for item in parsed
                    if isinstance(item, dict)
                    and item.get("tag") in valid_tags
                    and item.get("content")
                    and float(item.get("confidence", 0)) >= 0.6
                ]
            logger.warning(f"[extract_intelligence] parse failed: {result[:100]}")
        return []

    # ============================================================
    # Health & Fallback
    # ============================================================
    
    async def health_check(self) -> bool:
        """Check if Haiku API is reachable."""
        if not self.api_key:
            return False
        
        if not self.circuit_breaker.can_execute():
            return False
        
        try:
            ok = await get_llm_port().health_check()
            if ok:
                self.circuit_breaker.record_success()
            else:
                self.circuit_breaker.record_failure()
            return ok
        except Exception as e:
            logger.error(f"Haiku health check failed: {e}")
            self.circuit_breaker.record_failure()
            return False
    
    def _heuristic_classify(self, content: str) -> Dict[str, Any]:
        """Fallback heuristic classification when API is unavailable."""
        content_lower = content.lower()
        
        # Code indicators
        code_score = sum(1 for kw in ["def ", "class ", "import ", "return ", "async ", "await "]
                        if kw in content_lower)
        
        # Actions indicators
        action_score = sum(1 for kw in ["tool_call", "ssh_execute", "docker ", "curl ", "mkdir "]
                          if kw in content_lower)
        
        # Changes indicators
        change_score = sum(1 for kw in ["commit", "git ", "merge", "deploy", "config change"]
                          if kw in content_lower)
        
        scores = {"CODE": code_score, "ACTIONS": action_score, "CHANGES": change_score, "TEXT": 1}
        best_type = max(scores, key=scores.get)
        total = sum(scores.values()) or 1
        
        return {
            "type": best_type,
            "confidence": scores[best_type] / total,
            "reasoning": "heuristic_fallback",
        }
    
    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()


# Global service instance
haiku_service = HaikuService()


def get_haiku_service() -> HaikuService:
    """Get Haiku service instance"""
    return haiku_service
