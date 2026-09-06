"""
Confirmation Dispatcher
- Consume el canal transaccional prioritario (trade_confirmations) por una
  conexión pika DEDICADA (aislada del tráfico de mercado/orders_buffer).
- Deduplica con IdempotencyGuard (L1 cachetools + L2 Redis).
- Entrega la confirmación a comprador y vendedor por un canal push
  persistente (WebSocket), evitando el retardo estructural del polling.
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

app = FastAPI()
idempotency_guard = IdempotencyGuard()

# trader_id -> set de conexiones activas (un trader podría tener >1 pestaña/sesión)
connections: dict[str, set[WebSocket]] = defaultdict(set)
connections_lock = asyncio.Lock()

main_event_loop: asyncio.AbstractEventLoop | None = None


@app.on_event("startup")
async def startup():
    global main_event_loop
    main_event_loop = asyncio.get_running_loop()
    # El consumidor pika es bloqueante -> corre en un hilo dedicado, nunca
    # en el event loop de FastAPI.
    thread = threading.Thread(target=run_confirmation_consumer, daemon=True)
    thread.start()


@app.websocket("/ws/{trader_id}")
async def websocket_endpoint(websocket: WebSocket, trader_id: str):
    await websocket.accept()
    async with connections_lock:
        connections[trader_id].add(websocket)
    logger.info("Trader %s conectado (%d conexiones activas)", trader_id, len(connections[trader_id]))
    try:
        while True:
            # No esperamos mensajes del cliente; solo mantenemos el socket vivo.
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
        # Trader no conectado en este momento: la confirmación se pierde
        # a nivel de push (no a nivel de negocio, ya quedó deduplicada y
        # loggeada). Punto de extensión: guardar un buffer corto por
        # trader y reenviar al reconectar (outbox), si el negocio lo exige.
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
    """Se ejecuta en el hilo del consumidor pika. Agenda el envío async
    en el event loop principal de FastAPI de forma thread-safe."""
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


def run_confirmation_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(
        queue=CONFIRMATION_QUEUE,
        durable=True,
        arguments={'x-max-priority': 10},
    )
    # Prefetch bajo: esta cola es de baja latencia/alto valor por mensaje,
    # no de throughput masivo como orders_buffer.
    channel.basic_qos(prefetch_count=20)
    channel.basic_consume(queue=CONFIRMATION_QUEUE, on_message_callback=on_confirmation_received)
    logger.info("[*] Confirmation Dispatcher conectado a '%s' (conexión dedicada).", CONFIRMATION_QUEUE)
    channel.start_consuming()


@app.get("/health")
async def health():
    return {"status": "ok", "active_traders_connected": len(connections)}
