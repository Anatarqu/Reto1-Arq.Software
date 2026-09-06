"""
Motor de Idempotencia para el canal transaccional de confirmaciones.

Dos niveles:
  L1 - cachetools.TTLCache en proceso: evita ida y vuelta a Redis cuando
       RabbitMQ redelive­ra el mismo mensaje al mismo consumidor (caso más
       frecuente: nack/timeout local).
  L2 - Redis SET key value NX EX ttl: operación atómica de "check-and-set".
       Es la fuente de verdad; permite escalar el dispatcher a N réplicas
       sin que dos instancias despachen la misma confirmación dos veces.

Nota de diseño: si Redis no responde, se degrada a "best effort" usando
solo L1 (se registra un warning). Esto prioriza disponibilidad de la
confirmación sobre bloquear la ruta crítica por una dependencia caída;
documentar este trade-off es importante si el requisito de negocio fuera
"cero duplicados pase lo que pase" en vez de "cero duplicados en operación
normal".
"""
import logging
import os
import redis
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
        self._redis = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            socket_connect_timeout=0.5,
            socket_timeout=0.5,
            decode_responses=True,
        )

    def is_first_delivery(self, confirmation_id: str) -> bool:
        """
        Devuelve True solo la primera vez que ve este confirmation_id
        (en cualquier réplica del dispatcher). Devuelve False si ya fue
        procesado -> el llamador debe hacer ack sin volver a despachar.
        """
        # L1: camino rápido, sin red, para redeliveries locales
        if confirmation_id in self._l1:
            return False

        # L2: check-and-set atómico compartido
        try:
            acquired = self._redis.set(
                name=f"confirm:{confirmation_id}",
                value="1",
                nx=True,
                ex=IDEMPOTENCY_TTL_SECONDS,
            )
            self._l1[confirmation_id] = True
            return bool(acquired)
        except redis.RedisError as exc:
            logger.warning(
                "Redis no disponible para idempotencia (%s). "
                "Degradando a verificación local (L1) únicamente.",
                exc,
            )
            # Best-effort: si no está en L1, lo tratamos como primera vez
            # y lo marcamos localmente para no reenviarlo dentro de esta
            # misma réplica mientras Redis siga caído.
            self._l1[confirmation_id] = True
            return True
