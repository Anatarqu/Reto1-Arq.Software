import logging, os, threading, redis
from cachetools import TTLCache

logger = logging.getLogger("idempotency")
L1_CACHE_SIZE = int(os.getenv("L1_CACHE_SIZE", "50000"))
L1_CACHE_TTL_SECONDS = int(os.getenv("L1_CACHE_TTL_SECONDS", "120"))
IDEMPOTENCY_TTL_SECONDS = int(os.getenv("IDEMPOTENCY_TTL_SECONDS", "300"))
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

class IdempotencyGuard:
    def __init__(self):
        self._l1 = TTLCache(maxsize=L1_CACHE_SIZE, ttl=L1_CACHE_TTL_SECONDS)
        self._lock = threading.Lock()
        self._redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                                  socket_connect_timeout=0.5, socket_timeout=0.5,
                                  decode_responses=True)

    def is_first_delivery(self, confirmation_id: str) -> bool:
        with self._lock:
            if confirmation_id in self._l1:
                return False
        try:
            acquired = self._redis.set(
                name=f"confirm:{confirmation_id}", value="1",
                nx=True, ex=IDEMPOTENCY_TTL_SECONDS
            )
            with self._lock:
                self._l1[confirmation_id] = True
            return bool(acquired)
        except redis.RedisError as exc:
            logger.warning("[IDEMPOTENCY] Redis no disponible: %s", exc)
            with self._lock:
                if confirmation_id in self._l1:
                    return False
                self._l1[confirmation_id] = True
            return True
