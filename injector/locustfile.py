import os, time, uuid, random
from locust import HttpUser, task, between

# ---------------------------------------------------------------------------
# Configuración 100% MANUAL: número de usuarios y spawn rate se ajustan en
# la interfaz web de Locust (http://localhost:8089). WAIT_MIN/WAIT_MAX se
# ajustan aquí (o por variable de entorno) para calibrar la tasa TOTAL de
# órdenes/segundo que ese número de usuarios va a generar.
#
# Fórmula para elegir WAIT_MIN/WAIT_MAX dado un objetivo de tasa total:
#     espera_promedio_por_usuario = usuarios / tasa_objetivo_req_por_seg
#     WAIT_MIN = espera_promedio * 0.9   (aprox.)
#     WAIT_MAX = espera_promedio * 1.1
#
# Ejemplos con los objetivos del PDF:
#   Fase 1 (40 usuarios, ~1.300 órdenes/min = 21.67 req/s):
#     espera_promedio = 40 / 21.67 ≈ 1.85s -> WAIT_MIN=1.6 WAIT_MAX=2.0 (default)
#   Fase 2 (250 usuarios, ~6.500 órdenes/min = 108.33 req/s):
#     espera_promedio = 250 / 108.33 ≈ 2.31s -> WAIT_MIN=2.05 WAIT_MAX=2.55
#     docker compose run -e WAIT_MIN=2.05 -e WAIT_MAX=2.55 injector
# ---------------------------------------------------------------------------
WAIT_MIN = float(os.getenv("WAIT_MIN", "1.6"))
WAIT_MAX = float(os.getenv("WAIT_MAX", "2.0"))

class FinancialTrader(HttpUser):
    wait_time = between(WAIT_MIN, WAIT_MAX)

    def on_start(self):
        self.trader_id = f"TRADER_{random.randint(1, 1000)}"
        self.symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

    @task
    def post_order(self):
        # 800 BUY + 500 SELL = 1300 órdenes/min de referencia.
        side = "BUY" if random.random() < 800 / 1300 else "SELL"
        payload = {
            "order_id": str(uuid.uuid4()),
            "trader_id": self.trader_id,
            "symbol": random.choice(self.symbols),
            "side": side,
            "price": round(random.uniform(100, 1500), 2),
            "quantity": random.randint(10, 200),
            "timestamp": int(time.time() * 1000),
        }
        with self.client.post("/api/v1/orders", json=payload,
                              name="POST /api/v1/orders",
                              catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"HTTP {response.status_code}")
                return
            try:
                if response.json().get("status") != "ACK":
                    response.failure("Gateway no devolvió ACK")
            except Exception:
                response.failure("JSON inválido")
