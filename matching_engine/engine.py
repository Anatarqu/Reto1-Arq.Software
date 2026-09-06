import pika
import json
import heapq
import os
import time
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("engine")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")

ORDER_QUEUE = "orders_buffer"
CONFIRMATION_QUEUE = os.getenv("CONFIRMATION_QUEUE", "trade_confirmations")
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "100"))

CONNECTION_PARAMS = dict(
    heartbeat=60,
    blocked_connection_timeout=30,
    connection_attempts=10,
    retry_delay=2,
)

# Excepciones que indican que la conexión/canal murió y hay que reconectar,
# en vez de propagar y tumbar el proceso.
RECOVERABLE_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.ConnectionClosedByBroker,
    pika.exceptions.ChannelClosedByBroker,
    pika.exceptions.ChannelWrongStateError,
    pika.exceptions.StreamLostError,
    ConnectionError,
    OSError,
)


# ============================================================
# CONEXIÓN EXCLUSIVA PARA CONFIRMACIONES (canal transaccional)
# ============================================================

_confirm_state = {"connection": None, "channel": None}
_confirm_lock = threading.Lock()


def _connect_confirm_channel():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, **CONNECTION_PARAMS)
    )
    channel = connection.channel()
    channel.queue_declare(
        queue=CONFIRMATION_QUEUE,
        durable=True,
        arguments={"x-max-priority": 10},
    )
    channel.confirm_delivery()
    _confirm_state["connection"] = connection
    _confirm_state["channel"] = channel
    logger.info(f"[+] Canal de confirmaciones conectado ({CONFIRMATION_QUEUE})")


def _ensure_confirm_channel():
    connection = _confirm_state["connection"]
    channel = _confirm_state["channel"]

    if connection is not None and connection.is_open and channel is not None and channel.is_open:
        return

    logger.warning("[RECONNECT] Canal de confirmaciones cerrado, reconectando...")
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

    _connect_confirm_channel()


_connect_confirm_channel()


def publish_confirmation(payload: dict, max_retries: int = 3) -> bool:
    """
    Best-effort: intenta publicar la confirmación, reconectando el canal si
    hace falta. Devuelve False si no lo logra tras los reintentos, pero
    NUNCA lanza una excepción que pueda hacer que el llamador reprocese la
    orden original — el match ya ocurrió y ya está commiteado en el libro,
    eso no se puede (ni se debe) deshacer solo porque falló el aviso.
    """
    body = json.dumps(payload, separators=(",", ":"))

    for attempt in range(1, max_retries + 1):
        try:
            with _confirm_lock:
                _ensure_confirm_channel()
                channel = _confirm_state["channel"]

                published = channel.basic_publish(
                    exchange="",
                    routing_key=CONFIRMATION_QUEUE,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        priority=9,
                        content_type="application/json",
                    ),
                    mandatory=True,
                )

                if published is False:
                    raise RuntimeError("RabbitMQ no confirmó la publicación de la confirmación")

            return True

        except (pika.exceptions.UnroutableError, RuntimeError, *RECOVERABLE_ERRORS) as exc:
            logger.warning(f"[CONFIRM] Intento {attempt}/{max_retries} fallido: {exc}")
            time.sleep(0.05 * attempt)

    logger.error(f"[CONFIRM][ERROR] No se pudo publicar confirmation_id={payload.get('confirmation_id')} "
                 f"tras {max_retries} intentos. La orden SÍ quedó emparejada; solo se perdió el aviso.")
    return False


# ============================================================
# ORDER BOOK
# ============================================================

class OrderBookActor:
    def __init__(self, symbol):
        self.symbol = symbol
        self.buys = []
        self.sells = []
        self.batch_persistence = []
        self.match_seq = 0
        self.lock = threading.Lock()

    def process_order(self, order):
        with self.lock:
            ts_match_start = time.time_ns()
            side = order["side"]
            price = float(order["price"])
            qty = int(order["quantity"])
            matches_executed = 0

            if side == "BUY":
                while self.sells and self.sells[0][0] <= price and qty > 0:
                    sell_price, sell_order = heapq.heappop(self.sells)
                    match_qty = min(qty, sell_order["quantity"])
                    qty -= match_qty
                    sell_order["quantity"] -= match_qty
                    if sell_order["quantity"] > 0:
                        heapq.heappush(self.sells, (sell_price, sell_order))
                    matches_executed += 1
                    self._on_match(order, sell_order, match_qty)
                if qty > 0:
                    heapq.heappush(self.buys, (-price, order))
            else:
                while self.buys and -self.buys[0][0] >= price and qty > 0:
                    neg_buy_price, buy_order = heapq.heappop(self.buys)
                    match_qty = min(qty, buy_order["quantity"])
                    qty -= match_qty
                    buy_order["quantity"] -= match_qty
                    if buy_order["quantity"] > 0:
                        heapq.heappush(self.buys, (neg_buy_price, buy_order))
                    matches_executed += 1
                    self._on_match(buy_order, order, match_qty)
                if qty > 0:
                    heapq.heappush(self.sells, (price, order))

            ts_match_end = time.time_ns()
            core_latency_ms = (ts_match_end - ts_match_start) / 1_000_000.0
            total_pipeline_latency_ms = (ts_match_end - order["ts_ingest_start"]) / 1_000_000.0

            logger.info(f"[ENGINE] Symbol={self.symbol} Matches={matches_executed} "
                        f"Core={core_latency_ms:.2f}ms Pipeline={total_pipeline_latency_ms:.2f}ms")

    def _on_match(self, buy_order, sell_order, qty):
        ts_match_end = time.time_ns()

        match_event = {
            "buy_id": buy_order["order_id"],
            "sell_id": sell_order["order_id"],
            "quantity": qty,
            "ts": time.time(),
        }
        self.batch_persistence.append(match_event)
        if len(self.batch_persistence) >= 50:
            self.batch_persistence.clear()

        self.match_seq += 1
        confirmation_id = f"{self.symbol}-{buy_order['order_id']}-{sell_order['order_id']}-{self.match_seq}"

        confirmation_payload = {
            "confirmation_id": confirmation_id,
            "symbol": self.symbol,
            "quantity": qty,
            "buy_order_id": buy_order["order_id"],
            "buy_trader_id": buy_order["trader_id"],
            "sell_order_id": sell_order["order_id"],
            "sell_trader_id": sell_order["trader_id"],
            "ts_match_end": ts_match_end,
        }

        # IMPORTANTE: el match YA ocurrió y ya está commiteado en el libro
        # (heapq ya mutado). publish_confirmation es best-effort: si falla,
        # se loggea como error operacional, pero NUNCA se relanza una
        # excepción aquí — hacerlo tumbaría process_order(), lo que
        # provocaría un nack(requeue=True) de la orden original en
        # on_message_received() y la reprocesaría desde cero sobre un
        # libro que ya cambió, generando un segundo match espurio.
        publish_confirmation(confirmation_payload)


# ============================================================
# ACTORS
# ============================================================

actors = {}
actors_lock = threading.Lock()


def get_actor(symbol):
    if symbol not in actors:
        with actors_lock:
            if symbol not in actors:
                actors[symbol] = OrderBookActor(symbol)
    return actors[symbol]


# ============================================================
# CONSUMIDOR DE ÓRDENES (con reconexión automática)
# ============================================================

def on_message_received(ch, method, properties, body):
    try:
        order = json.loads(body)
        actor = get_actor(order["symbol"])
        actor.process_order(order)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as exc:
        # Esto ahora solo captura errores GENUINOS de procesamiento (JSON
        # malformado, símbolo inválido, etc.) — nunca fallos de publicación
        # de confirmación, que ya se manejan dentro de _on_match sin
        # relanzar. Por eso aquí sí es seguro nack+requeue: el libro de
        # órdenes NUNCA llegó a mutarse para este mensaje.
        logger.error(f"[ENGINE][ERROR] order={body[:200]} error={exc}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def _connect_and_declare_orders_channel():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, **CONNECTION_PARAMS)
    )
    channel = connection.channel()
    channel.queue_declare(
        queue=ORDER_QUEUE,
        durable=True,
        arguments={
            "x-max-length": 500000,
            "x-overflow": "reject-publish",
        },
    )
    channel.basic_qos(prefetch_count=PREFETCH_COUNT)
    channel.basic_consume(queue=ORDER_QUEUE, on_message_callback=on_message_received)
    return connection, channel


def run_consumer_forever():
    """
    Bucle exterior de reconexión: si la conexión de consumo se cae por
    cualquier motivo (reinicio del broker, corte de red, timeout de
    heartbeat), se reconecta y retoma el consumo en vez de morir el
    proceso completo. Esto es lo que permite sostener una corrida de
    30 minutos sin que un solo evento transitorio tumbe el experimento.
    """
    backoff_seconds = 1
    while True:
        try:
            connection, channel = _connect_and_declare_orders_channel()
            logger.info(f"[*] Matching Engine conectado. Prefetch={PREFETCH_COUNT} "
                        f"ConfirmationQueue={CONFIRMATION_QUEUE}")
            backoff_seconds = 1  # reconexión exitosa: resetear el backoff
            channel.start_consuming()
        except RECOVERABLE_ERRORS as exc:
            logger.warning(f"[RECONNECT] Conexión de órdenes perdida ({exc}); "
                            f"reintentando en {backoff_seconds}s...")
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 30)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    run_consumer_forever()
