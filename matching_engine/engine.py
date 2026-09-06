import pika, json, heapq, os, time, threading, logging, statistics
from queue import Queue, Full

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("engine")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
ORDER_QUEUE = os.getenv("ORDER_QUEUE", "orders_buffer")
CONFIRMATION_EXCHANGE = os.getenv("CONFIRMATION_EXCHANGE", "trade_events")
CONFIRMATION_QUEUE = os.getenv("CONFIRMATION_QUEUE", "trade_confirmations")
CONFIRMATION_ROUTING_KEY = os.getenv("CONFIRMATION_ROUTING_KEY", "trade.confirmation")
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "200"))
CONFIRMATION_BUFFER_SIZE = int(os.getenv("CONFIRMATION_BUFFER_SIZE", "100000"))

CONNECTION_PARAMS = dict(heartbeat=60, blocked_connection_timeout=30,
                         connection_attempts=10, retry_delay=2)
RECOVERABLE_ERRORS = (
    pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosedByBroker,
    pika.exceptions.ChannelClosedByBroker, pika.exceptions.ChannelWrongStateError,
    pika.exceptions.StreamLostError, ConnectionError, OSError,
)

confirmation_buffer = Queue(maxsize=CONFIRMATION_BUFFER_SIZE)
metrics_lock = threading.Lock()
metrics = {"orders": 0, "matches": 0, "core_ms": [], "started": time.time()}

def _declare_confirmation_topology(channel):
    channel.exchange_declare(exchange=CONFIRMATION_EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(
        queue=CONFIRMATION_QUEUE, durable=True,
        arguments={"x-max-priority": 10}
    )
    channel.queue_bind(exchange=CONFIRMATION_EXCHANGE, queue=CONFIRMATION_QUEUE,
                       routing_key=CONFIRMATION_ROUTING_KEY)

def _confirmation_publisher():
    backoff = 1
    while True:
        try:
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, **CONNECTION_PARAMS)
            )
            ch = conn.channel()
            _declare_confirmation_topology(ch)
            ch.confirm_delivery()
            logger.info("[ENGINE] Publicador transaccional conectado exchange=%s key=%s",
                        CONFIRMATION_EXCHANGE, CONFIRMATION_ROUTING_KEY)
            backoff = 1
            while True:
                payload = confirmation_buffer.get()
                try:
                    published = ch.basic_publish(
                        exchange=CONFIRMATION_EXCHANGE,
                        routing_key=CONFIRMATION_ROUTING_KEY,
                        body=json.dumps(payload, separators=(",", ":")),
                        properties=pika.BasicProperties(
                            delivery_mode=2, priority=9, content_type="application/json"
                        ),
                        mandatory=True,
                    )
                    if published is False:
                        raise RuntimeError("confirmación no confirmada")
                finally:
                    confirmation_buffer.task_done()
        except RECOVERABLE_ERRORS as exc:
            logger.warning("[ENGINE] canal transaccional perdido: %s; retry=%ss", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as exc:
            logger.exception("[ENGINE] error publicador: %s", exc)
            time.sleep(backoff)

threading.Thread(target=_confirmation_publisher, daemon=True).start()

class OrderBookActor:
    def __init__(self, symbol):
        self.symbol = symbol
        self.buys, self.sells = [], []
        self.match_seq = 0
        self.lock = threading.Lock()

    def process_order(self, order):
        pending = []
        core_start = time.perf_counter_ns()
        with self.lock:
            side = order["side"].upper()
            price = float(order["price"])
            qty = int(order["quantity"])
            if side == "BUY":
                while self.sells and self.sells[0][0] <= price and qty > 0:
                    sell_price, sell_order = heapq.heappop(self.sells)
                    match_qty = min(qty, sell_order["quantity"])
                    qty -= match_qty
                    sell_order["quantity"] -= match_qty
                    if sell_order["quantity"] > 0:
                        heapq.heappush(self.sells, (sell_price, sell_order))
                    pending.append((order, sell_order, match_qty))
                if qty > 0:
                    heapq.heappush(self.buys, (-price, order))
            else:
                while self.buys and -self.buys[0][0] >= price and qty > 0:
                    neg_price, buy_order = heapq.heappop(self.buys)
                    match_qty = min(qty, buy_order["quantity"])
                    qty -= match_qty
                    buy_order["quantity"] -= match_qty
                    if buy_order["quantity"] > 0:
                        heapq.heappush(self.buys, (neg_price, buy_order))
                    pending.append((buy_order, order, match_qty))

            core_end = time.perf_counter_ns()

        core_ms = (core_end - core_start) / 1_000_000
        with metrics_lock:
            metrics["orders"] += 1
            metrics["matches"] += len(pending)
            metrics["core_ms"].append(core_ms)
            if len(metrics["core_ms"]) > 10000:
                metrics["core_ms"] = metrics["core_ms"][-10000:]

        for buy_order, sell_order, qty in pending:
            self._emit_confirmation(buy_order, sell_order, qty)

        pipeline_ms = (time.time_ns() - order["ts_ingest_start"]) / 1_000_000
        logger.debug("[ENGINE] symbol=%s matches=%d core=%.3fms pipeline=%.3fms",
                     self.symbol, len(pending), core_ms, pipeline_ms)

    def _emit_confirmation(self, buy_order, sell_order, qty):
        self.match_seq += 1
        confirmation_id = f"{self.symbol}-{buy_order['order_id']}-{sell_order['order_id']}-{self.match_seq}"
        payload = {
            "confirmation_id": confirmation_id,
            "symbol": self.symbol, "quantity": qty,
            "buy_order_id": buy_order["order_id"], "buy_trader_id": buy_order["trader_id"],
            "sell_order_id": sell_order["order_id"], "sell_trader_id": sell_order["trader_id"],
            "ts_match_end": time.time_ns(),
        }
        while True:
            try:
                confirmation_buffer.put(payload, timeout=1)
                return
            except Full:
                logger.warning("[ENGINE] buffer de confirmaciones lleno; esperando sin perder evento")

actors, actors_lock = {}, threading.Lock()
def get_actor(symbol):
    with actors_lock:
        if symbol not in actors:
            actors[symbol] = OrderBookActor(symbol)
        return actors[symbol]

def on_message_received(ch, method, properties, body):
    try:
        order = json.loads(body)
        get_actor(order["symbol"]).process_order(order)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as exc:
        logger.exception("[ENGINE][ERROR] %s", exc)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def _connect_orders():
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, **CONNECTION_PARAMS))
    ch = conn.channel()
    ch.queue_declare(queue=ORDER_QUEUE, durable=True,
                     arguments={"x-max-length": 500000})
    ch.basic_qos(prefetch_count=PREFETCH_COUNT)
    ch.basic_consume(queue=ORDER_QUEUE, on_message_callback=on_message_received)
    return conn, ch

def _reporter():
    last_orders = last_matches = 0
    while True:
        time.sleep(60)
        with metrics_lock:
            orders = metrics["orders"]; matches = metrics["matches"]
            samples = list(metrics["core_ms"])
        p95 = statistics.quantiles(samples, n=20)[18] if len(samples) >= 20 else (max(samples) if samples else 0)
        logger.info("[ENGINE][1MIN] orders=%d orders/min matches=%d matches/min core_p95=%.3fms",
                    orders-last_orders, matches-last_matches, p95)
        last_orders, last_matches = orders, matches

threading.Thread(target=_reporter, daemon=True).start()

def run_consumer_forever():
    backoff = 1
    while True:
        try:
            conn, ch = _connect_orders()
            logger.info("[ENGINE] consumidor activo prefetch=%s", PREFETCH_COUNT)
            backoff = 1
            ch.start_consuming()
        except RECOVERABLE_ERRORS as exc:
            logger.warning("[ENGINE] consumidor perdido: %s; retry=%ss", exc, backoff)
            time.sleep(backoff); backoff = min(backoff * 2, 30)
        except KeyboardInterrupt:
            return

if __name__ == "__main__":
    run_consumer_forever()
