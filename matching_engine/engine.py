import pika
import json
import heapq
import os
import time
import threading


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")

ORDER_QUEUE = "orders_buffer"
CONFIRMATION_QUEUE = os.getenv(
    "CONFIRMATION_QUEUE",
    "trade_confirmations",
)

PREFETCH_COUNT = int(
    os.getenv("PREFETCH_COUNT", "100")
)


# ============================================================
# CONEXIÓN EXCLUSIVA PARA CONFIRMACIONES
# ============================================================

_confirm_connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        heartbeat=60,
        blocked_connection_timeout=30,
        connection_attempts=10,
        retry_delay=2,
    )
)

_confirm_channel = _confirm_connection.channel()


# Canal transaccional dedicado.
_confirm_channel.queue_declare(
    queue=CONFIRMATION_QUEUE,
    durable=True,
    arguments={
        "x-max-priority": 10,
    },
)

_confirm_channel.confirm_delivery()

_confirm_lock = threading.Lock()


def publish_confirmation(
    payload: dict,
    max_retries: int = 3,
) -> bool:

    body = json.dumps(
        payload,
        separators=(",", ":"),
    )

    for attempt in range(1, max_retries + 1):

        try:

            with _confirm_lock:

                published = _confirm_channel.basic_publish(
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
                    raise RuntimeError(
                        "RabbitMQ no confirmó confirmation"
                    )

            return True

        except (
            pika.exceptions.UnroutableError,
            pika.exceptions.AMQPError,
            RuntimeError,
        ) as exc:

            print(
                f"[CONFIRM] Intento "
                f"{attempt}/{max_retries} fallido: {exc}"
            )

            time.sleep(0.05 * attempt)

    print(
        "[CONFIRM][ERROR] No se pudo publicar "
        f"confirmation_id={payload.get('confirmation_id')}"
    )

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


            # ==================================================
            # BUY
            # ==================================================

            if side == "BUY":

                while (
                    self.sells
                    and self.sells[0][0] <= price
                    and qty > 0
                ):

                    sell_price, sell_order = heapq.heappop(
                        self.sells
                    )

                    match_qty = min(
                        qty,
                        sell_order["quantity"],
                    )

                    qty -= match_qty
                    sell_order["quantity"] -= match_qty

                    if sell_order["quantity"] > 0:

                        heapq.heappush(
                            self.sells,
                            (
                                sell_price,
                                sell_order,
                            ),
                        )

                    matches_executed += 1

                    self._on_match(
                        order,
                        sell_order,
                        match_qty,
                    )

                if qty > 0:

                    heapq.heappush(
                        self.buys,
                        (-price, order),
                    )


            # ==================================================
            # SELL
            # ==================================================

            else:

                while (
                    self.buys
                    and -self.buys[0][0] >= price
                    and qty > 0
                ):

                    neg_buy_price, buy_order = heapq.heappop(
                        self.buys
                    )

                    match_qty = min(
                        qty,
                        buy_order["quantity"],
                    )

                    qty -= match_qty
                    buy_order["quantity"] -= match_qty

                    if buy_order["quantity"] > 0:

                        heapq.heappush(
                            self.buys,
                            (
                                neg_buy_price,
                                buy_order,
                            ),
                        )

                    matches_executed += 1

                    self._on_match(
                        buy_order,
                        order,
                        match_qty,
                    )

                if qty > 0:

                    heapq.heappush(
                        self.sells,
                        (price, order),
                    )


            ts_match_end = time.time_ns()

            core_latency_ms = (
                ts_match_end - ts_match_start
            ) / 1_000_000.0

            total_pipeline_latency_ms = (
                ts_match_end
                - order["ts_ingest_start"]
            ) / 1_000_000.0


            print(
                f"[ENGINE] "
                f"Symbol={self.symbol} "
                f"Matches={matches_executed} "
                f"Core={core_latency_ms:.2f}ms "
                f"Pipeline={total_pipeline_latency_ms:.2f}ms"
            )


    def _on_match(
        self,
        buy_order,
        sell_order,
        qty,
    ):

        ts_match_end = time.time_ns()

        match_event = {
            "buy_id": buy_order["order_id"],
            "sell_id": sell_order["order_id"],
            "quantity": qty,
            "ts": time.time(),
        }

        self.batch_persistence.append(
            match_event
        )

        if len(self.batch_persistence) >= 50:

            self.batch_persistence.clear()


        self.match_seq += 1


        confirmation_id = (
            f"{self.symbol}-"
            f"{buy_order['order_id']}-"
            f"{sell_order['order_id']}-"
            f"{self.match_seq}"
        )


        confirmation_payload = {

            "confirmation_id": confirmation_id,

            "symbol": self.symbol,

            "quantity": qty,

            "buy_order_id":
                buy_order["order_id"],

            "buy_trader_id":
                buy_order["trader_id"],

            "sell_order_id":
                sell_order["order_id"],

            "sell_trader_id":
                sell_order["trader_id"],

            "ts_match_end": ts_match_end,

        }


        published = publish_confirmation(
            confirmation_payload
        )

        if not published:

            # No hacemos ACK de la orden si la confirmación
            # transaccional no pudo publicarse.
            raise RuntimeError(
                f"Confirmation perdida: "
                f"{confirmation_id}"
            )


# ============================================================
# ACTORS
# ============================================================

actors = {}

actors_lock = threading.Lock()


def get_actor(symbol):

    if symbol not in actors:

        with actors_lock:

            if symbol not in actors:

                actors[symbol] = OrderBookActor(
                    symbol
                )

    return actors[symbol]


# ============================================================
# CONSUMIDOR
# ============================================================

def on_message_received(
    ch,
    method,
    properties,
    body,
):

    order = json.loads(body)

    actor = get_actor(
        order["symbol"]
    )

    try:

        actor.process_order(order)

        ch.basic_ack(
            delivery_tag=method.delivery_tag
        )

    except Exception as exc:

        print(
            f"[ENGINE][ERROR] "
            f"order={order.get('order_id')} "
            f"error={exc}"
        )

        # No descartamos silenciosamente.
        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True,
        )


# ============================================================
# CONEXIÓN DE ÓRDENES
# ============================================================

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        heartbeat=60,
        blocked_connection_timeout=30,
        connection_attempts=10,
        retry_delay=2,
    )
)

channel = connection.channel()


# IMPORTANTE:
# Debe ser idéntica a la declaración del Gateway.
channel.queue_declare(
    queue=ORDER_QUEUE,
    durable=True,
    arguments={
        "x-max-length": 500000,
        "x-overflow": "reject-publish",
    },
)


channel.basic_qos(
    prefetch_count=PREFETCH_COUNT
)


channel.basic_consume(
    queue=ORDER_QUEUE,
    on_message_callback=on_message_received,
)


print(
    f"[*] Matching Engine conectado."
    f" Prefetch={PREFETCH_COUNT}"
    f" ConfirmationQueue={CONFIRMATION_QUEUE}"
)


channel.start_consuming()
