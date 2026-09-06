from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel
from contextlib import asynccontextmanager
import pika
import json
import time
import os
import threading

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')

# Singleton para mantener el canal global persistente
rabbitmq_client = {}
# pika.BlockingConnection/channel NO es thread-safe: run_in_threadpool puede
# ejecutar varias publicaciones concurrentes en hilos distintos, así que
# serializamos el acceso al canal con un lock. Esto no reintroduce el
# bloqueo del event loop (el lock se espera dentro del hilo del threadpool,
# no en el hilo del event loop), solo evita que dos hilos toquen el mismo
# canal pika a la vez.
_publish_lock = threading.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Código de inicialización al arrancar el contenedor
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    # IMPORTANTE: el nombre y los argumentos deben ser IDÉNTICOS a los que
    # declara matching_engine/engine.py para la misma cola. Si difieren,
    # RabbitMQ rechaza la declaración más reciente (406 PRECONDITION_FAILED)
    # y, si además el nombre difiere, gateway y engine terminan hablándole
    # a colas completamente distintas sin que nada lo reporte como error.
    channel.queue_declare(
        queue='orders_buffer',
        durable=True,
        arguments={
            'x-max-length': 500000,
            'x-overflow': 'reject-publish'
        }
    )
    rabbitmq_client['channel'] = channel
    rabbitmq_client['connection'] = connection
    print("[+] API Gateway: Conexión persistente a RabbitMQ establecida.")

    yield

    # Limpieza al detener el contenedor
    try:
        connection.close()
    except Exception:
        pass


# Inyección del ciclo de vida en FastAPI
app = FastAPI(lifespan=lifespan)


class OrderSchema(BaseModel):
    order_id: str
    trader_id: str
    symbol: str
    side: str
    price: float
    quantity: int
    timestamp: int


def _publish_blocking(payload: dict):
    """
    Llamada síncrona real a pika. Se ejecuta en un hilo del threadpool de
    Starlette (run_in_threadpool), nunca directamente en el event loop,
    para que una publicación lenta no congele el resto de las requests
    concurrentes que llegan al gateway.
    """
    channel = rabbitmq_client['channel']
    with _publish_lock:
        channel.basic_publish(
            exchange='',
            routing_key='orders_buffer',
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2)
        )


@app.post("/api/v1/orders")
async def ingest_order(order: OrderSchema):
    ts_ingest_start = time.time_ns()
    payload = order.model_dump()
    payload['ts_ingest_start'] = ts_ingest_start

    try:
        await run_in_threadpool(_publish_blocking, payload)
        ts_ingest_end = time.time_ns()
        latency_ms = (ts_ingest_end - ts_ingest_start) / 1_000_000.0
        return {"status": "ACK", "ingest_latency_ms": latency_ms}
    except Exception as e:
        raise HTTPException(status_code=503, detail="Buffer Saturated or Unreachable")
