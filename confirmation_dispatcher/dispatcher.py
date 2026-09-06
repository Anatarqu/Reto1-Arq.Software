import asyncio, json, logging, os, threading, time
from collections import defaultdict
import pika
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from idempotency import IdempotencyGuard

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dispatcher")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
CONFIRMATION_EXCHANGE = os.getenv("CONFIRMATION_EXCHANGE", "trade_events")
CONFIRMATION_QUEUE = os.getenv("CONFIRMATION_QUEUE", "trade_confirmations")
CONFIRMATION_ROUTING_KEY = os.getenv("CONFIRMATION_ROUTING_KEY", "trade.confirmation")

PARAMS = dict(heartbeat=60, blocked_connection_timeout=30,
              connection_attempts=10, retry_delay=2)
RECOVERABLE = (pika.exceptions.AMQPConnectionError,
               pika.exceptions.ConnectionClosedByBroker,
               pika.exceptions.ChannelClosedByBroker,
               pika.exceptions.ChannelWrongStateError,
               pika.exceptions.StreamLostError, ConnectionError, OSError)

app = FastAPI(title="Confirmation Dispatcher", version="2.0")
guard = IdempotencyGuard()
connections = defaultdict(set)
connections_lock = asyncio.Lock()
main_event_loop = None
consumer_status = {"connected": False, "last_error": None}
delivery_metrics = {"received": 0, "dispatched": 0, "duplicates": 0, "failed": 0}

@app.on_event("startup")
async def startup():
    global main_event_loop
    main_event_loop = asyncio.get_running_loop()
    threading.Thread(target=run_consumer_forever, daemon=True).start()

@app.websocket("/ws/{trader_id}")
async def websocket_endpoint(websocket: WebSocket, trader_id: str):
    await websocket.accept()
    async with connections_lock:
        connections[trader_id].add(websocket)
    logger.info("[WS] trader=%s connected", trader_id)
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

async def _send(trader_id, message):
    async with connections_lock:
        sockets = list(connections.get(trader_id, ()))
    if not sockets:
        logger.warning("[WS] trader=%s sin conexión; confirmation=%s",
                       trader_id, message["confirmation_id"])
        return
    payload = json.dumps(message, separators=(",", ":"))
    for ws in sockets:
        try:
            await ws.send_text(payload)
        except Exception as exc:
            logger.warning("[WS] send error trader=%s: %s", trader_id, exc)

def dispatch_confirmation(data):
    ts_dispatch = time.time_ns()
    latency_ms = (ts_dispatch - int(data["ts_match_end"])) / 1_000_000
    base = {
        "confirmation_id": data["confirmation_id"], "symbol": data["symbol"],
        "quantity": data["quantity"], "ts_match_end": data["ts_match_end"],
        "ts_dispatch": ts_dispatch, "dispatch_latency_ms": round(latency_ms, 3)
    }
    for trader_id, role, order_id in (
        (data["buy_trader_id"], "BUY", data["buy_order_id"]),
        (data["sell_trader_id"], "SELL", data["sell_order_id"]),
    ):
        msg = {**base, "role": role, "order_id": order_id}
        asyncio.run_coroutine_threadsafe(_send(trader_id, msg), main_event_loop)
    delivery_metrics["dispatched"] += 1
    logger.info("[DISPATCH] confirmation=%s latency=%.3fms",
                data["confirmation_id"], latency_ms)

def on_confirmation_received(ch, method, properties, body):
    try:
        data = json.loads(body)
        delivery_metrics["received"] += 1
        if guard.is_first_delivery(data["confirmation_id"]):
            dispatch_confirmation(data)
        else:
            delivery_metrics["duplicates"] += 1
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:
        delivery_metrics["failed"] += 1
        logger.exception("[DISPATCH][ERROR] nack without requeue")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def _connect():
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, **PARAMS))
    ch = conn.channel()
    ch.exchange_declare(exchange=CONFIRMATION_EXCHANGE, exchange_type="topic", durable=True)
    ch.queue_declare(queue=CONFIRMATION_QUEUE, durable=True,
                     arguments={"x-max-priority": 10})
    ch.queue_bind(exchange=CONFIRMATION_EXCHANGE, queue=CONFIRMATION_QUEUE,
                 routing_key=CONFIRMATION_ROUTING_KEY)
    ch.basic_qos(prefetch_count=100)
    ch.basic_consume(queue=CONFIRMATION_QUEUE, on_message_callback=on_confirmation_received)
    return conn, ch

def run_consumer_forever():
    backoff = 1
    while True:
        try:
            conn, ch = _connect()
            consumer_status["connected"] = True
            consumer_status["last_error"] = None
            logger.info("[DISPATCH] conectado exchange=%s queue=%s",
                        CONFIRMATION_EXCHANGE, CONFIRMATION_QUEUE)
            backoff = 1
            ch.start_consuming()
        except RECOVERABLE as exc:
            consumer_status["connected"] = False
            consumer_status["last_error"] = str(exc)
            time.sleep(backoff); backoff = min(backoff * 2, 30)
        except Exception as exc:
            consumer_status["connected"] = False
            consumer_status["last_error"] = str(exc)
            logger.exception("[DISPATCH] error inesperado")
            time.sleep(backoff); backoff = min(backoff * 2, 30)

@app.get("/health")
async def health():
    return {"status": "ok" if consumer_status["connected"] else "degraded",
            "consumer_connected": consumer_status["connected"],
            "consumer_last_error": consumer_status["last_error"],
            "metrics": delivery_metrics}
