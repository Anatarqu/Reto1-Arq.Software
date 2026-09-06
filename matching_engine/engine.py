import pika
import json
import heapq
import os
import time
import threading

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
PREFETCH_COUNT = int(os.getenv('PREFETCH_COUNT', '100'))
CONFIRMATION_QUEUE = os.getenv('CONFIRMATION_QUEUE', 'trade_confirmations')

# ---------------------------------------------------------------------------
# Canal transaccional prioritario: conexión pika DEDICADA, separada de la
# conexión que consume orders_buffer. Aunque hoy no exista todavía un canal
# de difusión de mercado, dejamos el aislamiento a nivel de conexión (no solo
# de cola) para que ninguna ampliación futura del tráfico de mercado pueda
# generar contención sobre las confirmaciones.
# ---------------------------------------------------------------------------
_confirm_connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RABBITMQ_HOST)
)
_confirm_channel = _confirm_connection.channel()
_confirm_channel.queue_declare(
    queue=CONFIRMATION_QUEUE,
    durable=True,
    arguments={'x-max-priority': 10}  # cola de prioridad nativa de RabbitMQ
)
_confirm_channel.confirm_delivery()  # publisher confirms: evita pérdida silenciosa
_confirm_lock = threading.Lock()


def publish_confirmation(payload: dict, max_retries: int = 3) -> bool:
    """
    Publica una confirmación transaccional en el canal prioritario.
    Usa publisher confirms + reintentos con el MISMO confirmation_id, para
    que el Verificador de Idempotencia del dispatcher pueda deduplicar
    correctamente si un reintento llega a entregarse dos veces.
    """
    body = json.dumps(payload)
    for attempt in range(1, max_retries + 1):
        try:
            with _confirm_lock:
                ok = _confirm_channel.basic_publish(
                    exchange='',
                    routing_key=CONFIRMATION_QUEUE,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,   # persistente
                        priority=9,        # alta prioridad dentro de la cola
                    ),
                    mandatory=True,
                )
            return True
        except (pika.exceptions.UnroutableError, pika.exceptions.AMQPError) as exc:
            print(f"[CONFIRM] Intento {attempt}/{max_retries} fallido: {exc}")
            time.sleep(0.05 * attempt)
    print(f"[CONFIRM][ERROR] No se pudo publicar confirmation_id="
          f"{payload.get('confirmation_id')} tras {max_retries} intentos.")
    return False


# Estructura del Modelo de Actores: Un hilo/estructura limpia por cada
# símbolo (Libro aislado sin bloqueos globales)
class OrderBookActor:
    def __init__(self, symbol):
        self.symbol = symbol
        self.buys = []   # Max-Heap (precios invertidos)
        self.sells = []  # Min-Heap
        self.batch_persistence = []
        self.match_seq = 0  # contador para confirmation_id determinístico
        self.lock = threading.Lock()

    def process_order(self, order):
        with self.lock:
            ts_match_start = time.time_ns()
            side = order['side']
            price = float(order['price'])
            qty = int(order['quantity'])
            matches_executed = 0

            if side == 'BUY':
                while self.sells and self.sells[0][0] <= price and qty > 0:
                    sell_price, sell_order = heapq.heappop(self.sells)
                    match_qty = min(qty, sell_order['quantity'])
                    qty -= match_qty
                    sell_order['quantity'] -= match_qty
                    if sell_order['quantity'] > 0:
                        heapq.heappush(self.sells, (sell_price, sell_order))
                    matches_executed += 1
                    self._on_match(order, sell_order, match_qty)
                if qty > 0:
                    heapq.heappush(self.buys, (-price, order))
            else:
                while self.buys and -self.buys[0][0] >= price and qty > 0:
                    neg_buy_price, buy_order = heapq.heappop(self.buys)
                    match_qty = min(qty, buy_order['quantity'])
                    qty -= match_qty
                    buy_order['quantity'] -= match_qty
                    if buy_order['quantity'] > 0:
                        heapq.heappush(self.buys, (neg_buy_price, buy_order))
                    matches_executed += 1
                    self._on_match(buy_order, order, match_qty)
                if qty > 0:
                    heapq.heappush(self.sells, (price, order))

            ts_match_end = time.time_ns()
            core_latency_ms = (ts_match_end - ts_match_start) / 1_000_000.0
            total_pipeline_latency_ms = (ts_match_end - order['ts_ingest_start']) / 1_000_000.0
            print(f"[ENGINE] Símbolo: {self.symbol} | Match: {matches_executed} | "
                  f"Latencia Core: {core_latency_ms:.2f}ms | "
                  f"Latencia End-to-End: {total_pipeline_latency_ms:.2f}ms")

    def _on_match(self, buy_order, sell_order, qty):
        ts_match_end = time.time_ns()

        # Táctica existente: escritura diferida por lotes (write-behind)
        match_event = {
            "buy_id": buy_order['order_id'],
            "sell_id": sell_order['order_id'],
            "quantity": qty,
            "ts": time.time(),
        }
        self.batch_persistence.append(match_event)
        if len(self.batch_persistence) >= 50:
            self.batch_persistence.clear()

        # Nuevo: publicación al canal transaccional de confirmaciones.
        # confirmation_id es determinístico (símbolo + ids + secuencia del
        # actor) y se calcula UNA sola vez: los reintentos de publish
        # reutilizan este mismo valor para no romper la idempotencia.
        self.match_seq += 1
        confirmation_id = f"{self.symbol}-{buy_order['order_id']}-{sell_order['order_id']}-{self.match_seq}"
        confirmation_payload = {
            "confirmation_id": confirmation_id,
            "symbol": self.symbol,
            "quantity": qty,
            "buy_order_id": buy_order['order_id'],
            "buy_trader_id": buy_order['trader_id'],
            "sell_order_id": sell_order['order_id'],
            "sell_trader_id": sell_order['trader_id'],
            "ts_match_end": ts_match_end,  # ns, usado para medir latencia end-to-end
        }
        publish_confirmation(confirmation_payload)


# Catálogo dinámico de Actores
actors = {}


def get_actor(symbol) -> OrderBookActor:
    if symbol not in actors:
        actors[symbol] = OrderBookActor(symbol)
    return actors[symbol]


def on_message_received(ch, method, properties, body):
    order = json.loads(body)
    actor = get_actor(order['symbol'])
    actor.process_order(order)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Configuración del Consumidor (conexión separada de la de confirmaciones)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue='orders_buffer', durable=True)
channel.basic_qos(prefetch_count=PREFETCH_COUNT)
channel.basic_consume(queue='orders_buffer', on_message_callback=on_message_received)

print(f"[*] Engine Conectado. Prefetch Count: {PREFETCH_COUNT}. "
      f"Canal de confirmaciones: '{CONFIRMATION_QUEUE}' (conexión dedicada).")
channel.start_consuming()
