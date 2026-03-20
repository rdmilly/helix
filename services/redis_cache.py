"""Redis Cache — Phase 6.

Provides:
  - Queue hot-path: LPUSH on intake, BLPOP in worker (falls back to PG)
  - Session hot cache: GET/SET with 1hr TTL
  - Circuit breaker: 3 failures -> 60s cooldown, all ops degrade to PG

Redis key schema:
  helix:queue          LIST  queue_ids in priority order (LPUSH = head)
  helix:session:{id}   STRING  JSON session meta (TTL 3600)
  helix:lock:{id}      STRING  processing lock (TTL 30s)
"""
import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://helix-redis:6379")
QUEUE_KEY  = "helix:queue"
SESSION_TTL = 3600   # 1 hour
LOCK_TTL    = 30     # 30 seconds


class CircuitBreaker:
    def __init__(self, threshold: int = 3, timeout: int = 60):
        self.threshold = threshold
        self.timeout   = timeout
        self.failures  = 0
        self.last_fail = 0.0
        self.open      = False

    def record_failure(self):
        self.failures += 1
        self.last_fail  = time.time()
        if self.failures >= self.threshold:
            self.open = True
            logger.warning("Redis circuit breaker OPEN")

    def record_success(self):
        if self.failures > 0:
            self.failures = 0
        if self.open:
            self.open = False
            logger.info("Redis circuit breaker CLOSED")

    def can_execute(self) -> bool:
        if not self.open:
            return True
        if time.time() - self.last_fail > self.timeout:
            self.open = False
            return True
        return False


class RedisCache:
    """
    Async Redis client wrapping redis.asyncio.
    All methods are safe to call even if Redis is down —
    they return None/False/[] and log a warning.
    """

    def __init__(self):
        self._client = None
        self._initialized = False
        self.cb = CircuitBreaker()

    async def initialize(self) -> bool:
        if not self.cb.can_execute():
            return False
        try:
            import redis.asyncio as aioredis
            self._client = aioredis.from_url(
                REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=3,
            )
            await self._client.ping()
            self._initialized = True
            self.cb.record_success()
            logger.info(f"Redis connected: {REDIS_URL}")
            return True
        except Exception as e:
            logger.error(f"Redis init failed: {e}")
            self.cb.record_failure()
            return False

    async def close(self):
        if self._client:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None
            self._initialized = False

    def _ok(self) -> bool:
        return self._initialized and self._client is not None and self.cb.can_execute()

    # ------------------------------------------------------------------
    # Queue operations
    # ------------------------------------------------------------------

    async def queue_push(self, queue_id: str, priority: int = 0) -> bool:
        """Push a queue item ID onto the Redis list.
        Higher priority -> pushed to head (LPUSH).
        Normal priority -> pushed to tail (RPUSH).
        """
        if not self._ok():
            return False
        try:
            if priority > 0:
                await self._client.lpush(QUEUE_KEY, queue_id)
            else:
                await self._client.rpush(QUEUE_KEY, queue_id)
            self.cb.record_success()
            return True
        except Exception as e:
            logger.warning(f"Redis queue_push failed: {e}")
            self.cb.record_failure()
            return False

    async def queue_pop(self, timeout: int = 1) -> Optional[str]:
        """Pop a queue item ID (blocking, timeout seconds). Returns None on timeout/error."""
        if not self._ok():
            return None
        try:
            result = await self._client.blpop(QUEUE_KEY, timeout=timeout)
            self.cb.record_success()
            if result:
                return result[1]  # (key, value)
            return None
        except Exception as e:
            logger.warning(f"Redis queue_pop failed: {e}")
            self.cb.record_failure()
            return None

    async def queue_remove(self, queue_id: str) -> bool:
        """Remove a specific queue item ID (used on complete/fail)."""
        if not self._ok():
            return False
        try:
            await self._client.lrem(QUEUE_KEY, 0, queue_id)
            self.cb.record_success()
            return True
        except Exception as e:
            logger.warning(f"Redis queue_remove failed: {e}")
            self.cb.record_failure()
            return False

    async def queue_length(self) -> int:
        """Return current Redis queue depth."""
        if not self._ok():
            return -1
        try:
            n = await self._client.llen(QUEUE_KEY)
            self.cb.record_success()
            return n
        except Exception as e:
            logger.warning(f"Redis queue_length failed: {e}")
            self.cb.record_failure()
            return -1

    # ------------------------------------------------------------------
    # Session hot cache
    # ------------------------------------------------------------------

    async def session_set(self, session_id: str, meta: Dict[str, Any]) -> bool:
        """Cache session meta with TTL."""
        if not self._ok():
            return False
        try:
            key = f"helix:session:{session_id}"
            await self._client.setex(key, SESSION_TTL, json.dumps(meta))
            self.cb.record_success()
            return True
        except Exception as e:
            logger.warning(f"Redis session_set failed: {e}")
            self.cb.record_failure()
            return False

    async def session_get(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached session meta, or None if not cached."""
        if not self._ok():
            return None
        try:
            key = f"helix:session:{session_id}"
            val = await self._client.get(key)
            self.cb.record_success()
            return json.loads(val) if val else None
        except Exception as e:
            logger.warning(f"Redis session_get failed: {e}")
            self.cb.record_failure()
            return None

    async def session_invalidate(self, session_id: str) -> bool:
        """Remove session from cache."""
        if not self._ok():
            return False
        try:
            await self._client.delete(f"helix:session:{session_id}")
            self.cb.record_success()
            return True
        except Exception as e:
            logger.warning(f"Redis session_invalidate failed: {e}")
            self.cb.record_failure()
            return False

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    async def stats(self) -> Dict[str, Any]:
        if not self._ok():
            return {"available": False}
        try:
            info = await self._client.info("memory")
            qlen = await self._client.llen(QUEUE_KEY)
            self.cb.record_success()
            return {
                "available":    True,
                "queue_depth":  qlen,
                "used_memory":  info.get("used_memory_human", "?"),
                "max_memory":   info.get("maxmemory_human", "?"),
            }
        except Exception as e:
            logger.warning(f"Redis stats failed: {e}")
            self.cb.record_failure()
            return {"available": False, "error": str(e)}


# Global singleton
_redis_cache: Optional[RedisCache] = None


def get_redis_cache() -> RedisCache:
    global _redis_cache
    if _redis_cache is None:
        _redis_cache = RedisCache()
    return _redis_cache
