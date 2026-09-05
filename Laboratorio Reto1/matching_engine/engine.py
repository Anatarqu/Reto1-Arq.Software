import pika
import json
import heapq
import os
import time
import threading

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
PREFETCH_COUNT = int(os.getenv('PREFETCH_COUNT', '100'))

class OrderBookActor:
    def __init__(self, symbol):
        self.symbol = symbol
        self.buys = []   # Max-Heap (precios invertidos: -precio)
        self.sells = []  # Min-Heap (precios normales: precio)
        self.batch_persistence = []
        self.lock = threading.Lock()

    def process_order(self, order):
        with self.lock:
            ts_match_start = time.time_ns()
            side = order['side']
            price = float(order['price'])
            qty = int(order['quantity'])
            matches_executed = 0
            
            if side == 'BUY':
                # Validar contra la orden de venta más barata en la raíz del heap: self.sells[0][0]
                while self.sells and self.sells[0][0] <= price and qty > 0:
                    sell_price, sell_order = heapq.heappop(self.sells)
                    match_qty = min(qty, sell_order['quantity'])
                    qty -= match_qty
                    sell_order['quantity'] -= match_qty
                    
                    if sell_order['quantity'] > 0:
                        heapq.heappush(self.sells, (sell_price, sell_order))
                    matches_executed += 1
                    self._trigger_write_behind(order, sell_order, match_qty)
                    
                if qty > 0:
                    # Guardar precio invertido para simular el Max-Heap de compras
                    heapq.heappush(self.buys, (-price, order))
            else:
                # Validar contra la orden de compra más cara en la raíz del heap: -self.buys[0][0]
                while self.buys and (-self.buys[0][0]) >= price and qty > 0:
                    neg_buy_price, buy_order = heapq.heappop(self.buys)
                    match_qty = min(qty, buy_order['quantity'])
                    qty -= match_qty
                    buy_order['quantity'] -= match_qty
                    
                    if buy_order['quantity'] > 0:
                        heapq.heappush(self.buys, (neg_buy_price, buy_order))
                    matches_executed += 1
                    self._trigger_write_behind(buy_order, order, match_qty)
                    
                if qty > 0:
                    heapq.heappush(self.sells, (price, order))

            ts_match_end = time.time_ns()
            core_latency_ms = (ts_match_end - ts_match_start) / 1_000_000.0
            total_pipeline_latency_ms = (ts_match_end - order['ts_ingest_start']) / 1_000_000.0
            
            if matches_executed > 0:
                print(f"[ENGINE] Símbolo: {self.symbol} | Cruces: {matches_executed} | Latencia Core: {core_latency_ms:.2f}ms | Latencia E2E: {total_pipeline_latency_ms:.2f}ms")

    def _trigger_write_behind(self, buy_order, sell_order, qty):
        match_event = {"buy_id": buy_order['order_id'], "sell_id": sell_order['order_id'], "quantity": qty, "ts": time.time()}
        self.batch_persistence.append(match_event)
        if len(self.batch_persistence) >= 50:
            self.batch_persistence.clear()

actors = {}
def get_actor(symbol) -> OrderBookActor:
    if symbol not in actors:
        actors[symbol] = OrderBookActor(symbol)
    return actors[symbol]

def on_message_received(ch, method, properties, body):
    try:
        order = json.loads(body)
        actor = get_actor(order['symbol'])
        actor.process_order(order)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[ERROR CRÍTICO] Fallo procesando orden: {e}")
        # Rechaza el mensaje y vuelve a encolarlo para evitar pérdidas de datos
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

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

channel.basic_qos(prefetch_count=PREFETCH_COUNT)
channel.basic_consume(queue='orders_buffer_clean', on_message_callback=on_message_received)

print(f"[*] Engine Conectado Exitosamente. Procesando en RAM libre de bloqueos...")
channel.start_consuming()