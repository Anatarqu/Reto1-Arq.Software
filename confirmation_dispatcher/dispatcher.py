"""
Confirmation Dispatcher
- Consume el canal transaccional prioritario (trade_confirmations) por una
  conexión pika DEDICADA (aislada del tráfico de mercado/orders_buffer).
- Deduplica con IdempotencyGuard (L1 cachetools + L2 Redis).
- Entrega la confirmación a comprador y vendedor por un canal push
  persistente (WebSocket), evitando el retardo estructural del polling.
- Reconecta automáticamente si la conexión con RabbitMQ se cae: antes, si
  esto pasaba, el hilo consumidor moría en silencio (era un daemon thread)
  y /health seguía devolviendo "ok" aunque ya no se procesara nada más.
"""
import asyncio
import json
import logging
import os
import threading
import time
from collections import defaultdict

import pika
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from idempotency import IdempotencyGuard

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dispatcher")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
CONFIRMATION_QUEUE = os.getenv("CONFIRMATION_QUEUE", "trade_confirmations")

CONNECTION_PARAMS = dict(
    heartbeat=60,
    blocked_connection_timeout=30,
    connection_attempts=10,
    retry_delay=2,
)

RECOVERABLE_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.ConnectionClosedByBroker,
    pika.exceptions.ChannelClosedByBroker,
    pika.exceptions.ChannelWrongStateError,
    pika.exceptions.StreamLostError,
    ConnectionError,
    OSError,
)

app = FastAPI()
idempotency_guard = IdempotencyGuard()

# trader_id -> set de conexiones activas (un trader podría tener >1 pestaña/sesión)
connections: dict[str, set[WebSocket]] = defaultdict(set)
connections_lock = asyncio.Lock()

main_event_loop: asyncio.AbstractEventLoop | None = None

# Se actualiza en cada reconexión exitosa; expuesto en /health para que sea
# visible desde afuera si el consumidor sigue vivo o lleva rato caído.
consumer_status = {"connected": False, "last_error": None}


@app.on_event("startup")
async def startup():
    global main_event_loop
    main_event_loop = asyncio.get_running_loop()
    # El consumidor pika es bloqueante -> corre en un hilo dedicado, nunca
    # en el event loop de FastAPI.
    thread = threading.Thread(target=run_consumer_forever, daemon=True)
    thread.start()


@app.websocket("/ws/{trader_id}")
async def websocket_endpoint(websocket: WebSocket, trader_id: str):
    await websocket.accept()
    async with connections_lock:
        connections[trader_id].add(websocket)
    logger.info("Trader %s conectado (%d conexiones activas)", trader_id, len(connections[trader_id]))
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        async with connections_lock:
            connections[trader_id].discard(websocket)
            if not connections[trader_id]:
                del connections[trader_id]


async def _send_to_trader(trader_id: str, message: dict):
    async with connections_lock:
        sockets = list(connections.get(trader_id, ()))
    if not sockets:
        logger.warning("Trader %s sin conexión activa; confirmación %s no entregada por push.",
                        trader_id, message.get("confirmation_id"))
        return
    payload = json.dumps(message)
    for ws in sockets:
        try:
            await ws.send_text(payload)
        except Exception as exc:
            logger.warning("Fallo enviando a trader %s: %s", trader_id, exc)


def dispatch_confirmation(order_data: dict):
    ts_dispatch = time.time_ns()
    latency_ms = (ts_dispatch - order_data["ts_match_end"]) / 1_000_000.0

    base = {
        "confirmation_id": order_data["confirmation_id"],
        "symbol": order_data["symbol"],
        "quantity": order_data["quantity"],
        "ts_match_end": order_data["ts_match_end"],
        "ts_dispatch": ts_dispatch,
        "dispatch_latency_ms": round(latency_ms, 3),
    }

    buy_msg = {**base, "role": "BUY", "order_id": order_data["buy_order_id"]}
    sell_msg = {**base, "role": "SELL", "order_id": order_data["sell_order_id"]}

    for trader_id, msg in (
        (order_data["buy_trader_id"], buy_msg),
        (order_data["sell_trader_id"], sell_msg),
    ):
        asyncio.run_coroutine_threadsafe(_send_to_trader(trader_id, msg), main_event_loop)

    logger.info("[DISPATCH] %s | latencia dispatcher: %.2fms",
                order_data["confirmation_id"], latency_ms)


def on_confirmation_received(ch, method, properties, body):
    try:
        order_data = json.loads(body)
        confirmation_id = order_data["confirmation_id"]

        if idempotency_guard.is_first_delivery(confirmation_id):
            dispatch_confirmation(order_data)
        else:
            logger.info("[IDEMPOTENCY] Confirmación duplicada descartada: %s", confirmation_id)

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:
        logger.exception("Error procesando confirmación; se hace nack sin requeue para evitar loop infinito")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def _connect_and_declare():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, **CONNECTION_PARAMS)
    )
    channel = connection.channel()
    channel.queue_declare(
        queue=CONFIRMATION_QUEUE,
        durable=True,
        arguments={"x-max-priority": 10},
    )
    channel.basic_qos(prefetch_count=20)
    channel.basic_consume(queue=CONFIRMATION_QUEUE, on_message_callback=on_confirmation_received)
    return connection, channel


def run_consumer_forever():
    """
    Bucle exterior de reconexión, igual que en matching_engine. Si la
    conexión muere, se reconecta y retoma el consumo en vez de dejar el
    hilo daemon morir en silencio.
    """
    backoff_seconds = 1
    while True:
        try:
            connection, channel = _connect_and_declare()
            logger.info("[*] Confirmation Dispatcher conectado a '%s' (conexión dedicada).",
                        CONFIRMATION_QUEUE)
            consumer_status["connected"] = True
            consumer_status["last_error"] = None
            backoff_seconds = 1
            channel.start_consuming()
        except RECOVERABLE_ERRORS as exc:
            consumer_status["connected"] = False
            consumer_status["last_error"] = str(exc)
            logger.warning("[RECONNECT] Conexión de confirmaciones perdida (%s); "
                            "reintentando en %ss...", exc, backoff_seconds)
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 30)
        except Exception as exc:
            # Cualquier otro error inesperado tampoco debe matar el hilo
            # en silencio: se loggea, se marca el estado y se reintenta.
            consumer_status["connected"] = False
            consumer_status["last_error"] = str(exc)
            logger.exception("[RECONNECT] Error inesperado en el consumidor; reintentando en %ss...",
                              backoff_seconds)
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 30)


@app.get("/health")
async def health():
    return {
        "status": "ok" if consumer_status["connected"] else "degraded",
        "consumer_connected": consumer_status["connected"],
        "consumer_last_error": consumer_status["last_error"],
        "active_traders_connected": len(connections),
    }
