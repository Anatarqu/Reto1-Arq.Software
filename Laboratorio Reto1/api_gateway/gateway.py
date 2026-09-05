from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
import pika
import json
import time
import os

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')

# Singleton para mantener el canal global persistente
rabbitmq_client = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Código de inicialización al arrancar el contenedor
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(
        queue='orders_buffer_clean', 
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

@app.post("/api/v1/orders")
async def ingest_order(order: OrderSchema):
    ts_ingest_start = time.time_ns()
    payload = order.model_dump()
    payload['ts_ingest_start'] = ts_ingest_start
    
    try:
        channel = rabbitmq_client['channel']
        channel.basic_publish(
            exchange='',
            routing_key='orders_buffer_clean',
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        ts_ingest_end = time.time_ns()
        latency_ms = (ts_ingest_end - ts_ingest_start) / 1_000_000.0
        return {"status": "ACK", "ingest_latency_ms": latency_ms}
    except Exception as e:
        raise HTTPException(status_code=503, detail="Buffer Saturated or Unreachable")