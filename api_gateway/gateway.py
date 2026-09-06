from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel
from contextlib import asynccontextmanager

import pika
import json
import time
import os
import threading


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")

ORDER_QUEUE = "orders_buffer"
ORDER_QUEUE_MAX_LENGTH = 500000

rabbitmq_client = {}
_publish_lock = threading.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):

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

    # Publisher confirms.
    channel.confirm_delivery()

    rabbitmq_client["channel"] = channel
    rabbitmq_client["connection"] = connection

    print(
        f"[+] API Gateway conectado a RabbitMQ "
        f"queue={ORDER_QUEUE}"
    )

    yield

    try:
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

    channel = rabbitmq_client["channel"]

    with _publish_lock:

        published = channel.basic_publish(
            exchange="",
            routing_key=ORDER_QUEUE,
            body=json.dumps(
                payload,
                separators=(",", ":"),
            ),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
            mandatory=True,
        )

        if published is False:
            raise RuntimeError(
                "RabbitMQ no confirmó la publicación"
            )


@app.get("/health")
async def health():

    connection = rabbitmq_client.get("connection")

    if connection is None or connection.is_closed:
        raise HTTPException(
            status_code=503,
            detail="RabbitMQ unavailable",
        )

    return {
        "status": "UP",
        "service": "api-gateway",
        "queue": ORDER_QUEUE,
    }


@app.post("/api/v1/orders")
async def ingest_order(order: OrderSchema):

    ts_ingest_start = time.time_ns()

    payload = order.model_dump()

    payload["ts_ingest_start"] = ts_ingest_start

    try:

        await run_in_threadpool(
            _publish_blocking,
            payload,
        )

        ts_ingest_end = time.time_ns()

        latency_ms = (
            ts_ingest_end - ts_ingest_start
        ) / 1_000_000.0

        return {
            "status": "ACK",
            "order_id": order.order_id,
            "ingest_latency_ms": latency_ms,
        }

    except Exception as exc:

        print(
            f"[GATEWAY][ERROR] "
            f"order={order.order_id} "
            f"error={exc}"
        )

        raise HTTPException(
            status_code=503,
            detail="Buffer saturated or RabbitMQ unavailable",
        )
