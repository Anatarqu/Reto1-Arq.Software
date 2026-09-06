from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel
from contextlib import asynccontextmanager

import pika
import json
import time
import os
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")

ORDER_QUEUE = "orders_buffer"
ORDER_QUEUE_MAX_LENGTH = 500000

rabbitmq_client = {}
_publish_lock = threading.Lock()


def _connect():
    """
    Crea una conexión + canal nuevos y deja todo declarado exactamente igual
    que matching_engine/engine.py. Se llama tanto al arrancar (lifespan)
    como cada vez que se detecta que la conexión anterior murió.
    """
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        heartbeat=60,
        blocked_connection_timeout=30,
        connection_attempts=10,
        retry_delay=2,
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # EXACTAMENTE la misma configuración que utiliza matching-engine.
    channel.queue_declare(
        queue=ORDER_QUEUE,
        durable=True,
        arguments={
            "x-max-length": ORDER_QUEUE_MAX_LENGTH,
            "x-overflow": "reject-publish",
        },
    )

    channel.confirm_delivery()

    rabbitmq_client["connection"] = connection
    rabbitmq_client["channel"] = channel

    logger.info(f"[+] API Gateway conectado a RabbitMQ queue={ORDER_QUEUE}")


def _ensure_connection():
    """
    Se llama SIEMPRE antes de publicar, dentro de _publish_lock. Si la
    conexión/canal actuales ya no sirven (se cerraron por un reinicio de
    RabbitMQ, un timeout de heartbeat bajo carga, un corte de red, etc.),
    los reemplaza por unos nuevos. Esto es lo que evita que un solo evento
    de red mate el gateway para el resto del experimento — antes, una vez
    cerrado el canal, TODAS las requests siguientes fallaban para siempre.
    """
    connection = rabbitmq_client.get("connection")
    channel = rabbitmq_client.get("channel")

    if connection is not None and connection.is_open and channel is not None and channel.is_open:
        return

    logger.warning("[RECONNECT] Conexión/canal de RabbitMQ cerrados, reconectando...")

    # Limpieza best-effort de lo que haya quedado vivo.
    try:
        if channel is not None and channel.is_open:
            channel.close()
    except Exception:
        pass
    try:
        if connection is not None and connection.is_open:
            connection.close()
    except Exception:
        pass

    _connect()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _connect()
    yield
    try:
        connection = rabbitmq_client.get("connection")
        if connection is not None:
            connection.close()
    except Exception:
        pass


app = FastAPI(
    title="Trading Transaction Gateway",
    version="1.0.0",
    lifespan=lifespan,
)


class OrderSchema(BaseModel):
    order_id: str
    trader_id: str
    symbol: str
    side: str
    price: float
    quantity: int
    timestamp: int


def _publish_blocking(payload: dict):
    with _publish_lock:
        _ensure_connection()

        channel = rabbitmq_client["channel"]

        published = channel.basic_publish(
            exchange="",
            routing_key=ORDER_QUEUE,
            body=json.dumps(payload, separators=(",", ":")),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
            mandatory=True,
        )

        if published is False:
            raise RuntimeError("RabbitMQ no confirmó la publicación")


@app.get("/health")
async def health():
    connection = rabbitmq_client.get("connection")
    channel = rabbitmq_client.get("channel")

    if connection is None or connection.is_closed or channel is None or channel.is_closed:
        raise HTTPException(status_code=503, detail="RabbitMQ unavailable")

    return {"status": "UP", "service": "api-gateway", "queue": ORDER_QUEUE}


@app.post("/api/v1/orders")
async def ingest_order(order: OrderSchema):
    ts_ingest_start = time.time_ns()
    payload = order.model_dump()
    payload["ts_ingest_start"] = ts_ingest_start

    try:
        await run_in_threadpool(_publish_blocking, payload)

        ts_ingest_end = time.time_ns()
        latency_ms = (ts_ingest_end - ts_ingest_start) / 1_000_000.0

        return {
            "status": "ACK",
            "order_id": order.order_id,
            "ingest_latency_ms": latency_ms,
        }

    except Exception as exc:
        logger.error(f"[GATEWAY][ERROR] order={order.order_id} error={exc}")
        raise HTTPException(status_code=503, detail="Buffer saturated or RabbitMQ unavailable")
