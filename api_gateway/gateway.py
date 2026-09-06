from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel
from contextlib import asynccontextmanager
import pika, json, time, os, threading, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
ORDER_QUEUE = os.getenv("ORDER_QUEUE", "orders_buffer")
ORDER_QUEUE_MAX_LENGTH = int(os.getenv("ORDER_QUEUE_MAX_LENGTH", "500000"))

rabbitmq_client = {}
_publish_lock = threading.Lock()

CONNECTION_PARAMS = dict(
    heartbeat=60, blocked_connection_timeout=30,
    connection_attempts=10, retry_delay=2,
)

def _declare_order_queue(channel):
    channel.queue_declare(
        queue=ORDER_QUEUE, durable=True,
        arguments={"x-max-length": ORDER_QUEUE_MAX_LENGTH},
    )

def _connect():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, **CONNECTION_PARAMS)
    )
    channel = connection.channel()
    _declare_order_queue(channel)
    channel.confirm_delivery()
    rabbitmq_client["connection"] = connection
    rabbitmq_client["channel"] = channel
    logger.info("[GATEWAY] RabbitMQ conectado queue=%s max=%s",
                ORDER_QUEUE, ORDER_QUEUE_MAX_LENGTH)

def _ensure_connection():
    connection = rabbitmq_client.get("connection")
    channel = rabbitmq_client.get("channel")
    if connection and connection.is_open and channel and channel.is_open:
        return
    try:
        if channel and channel.is_open:
            channel.close()
    except Exception:
        pass
    try:
        if connection and connection.is_open:
            connection.close()
    except Exception:
        pass
    _connect()

@asynccontextmanager
async def lifespan(app: FastAPI):
    _connect()
    stop = threading.Event()

    def heartbeat():
        while not stop.wait(15):
            with _publish_lock:
                try:
                    conn = rabbitmq_client.get("connection")
                    if conn and conn.is_open:
                        conn.process_data_events(time_limit=0)
                except Exception as exc:
                    logger.warning("[GATEWAY] heartbeat: %s", exc)

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    yield
    stop.set()
    try:
        conn = rabbitmq_client.get("connection")
        if conn and conn.is_open:
            conn.close()
    except Exception:
        pass

app = FastAPI(title="Trading Transaction Gateway", version="2.0", lifespan=lifespan)

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
                delivery_mode=2, content_type="application/json"
            ),
            mandatory=True,
        )
        if published is False:
            raise RuntimeError("RabbitMQ no confirmó la orden")

@app.get("/health")
async def health():
    conn = rabbitmq_client.get("connection")
    channel = rabbitmq_client.get("channel")
    if not conn or conn.is_closed or not channel or channel.is_closed:
        raise HTTPException(status_code=503, detail="RabbitMQ unavailable")
    return {"status": "UP", "service": "api-gateway", "queue": ORDER_QUEUE}

@app.post("/api/v1/orders")
async def ingest_order(order: OrderSchema):
    start = time.perf_counter_ns()
    payload = order.model_dump()
    payload["ts_ingest_start"] = time.time_ns()
    try:
        await run_in_threadpool(_publish_blocking, payload)
        latency_ms = (time.perf_counter_ns() - start) / 1_000_000
        return {"status": "ACK", "order_id": order.order_id,
                "ingest_latency_ms": round(latency_ms, 3)}
    except Exception as exc:
        logger.error("[GATEWAY][ERROR] order=%s error=%s", order.order_id, exc)
        raise HTTPException(status_code=503, detail="Order buffer unavailable")
