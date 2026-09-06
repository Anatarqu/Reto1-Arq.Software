import os, time, uuid, random
from locust import HttpUser, task, between

TARGET_RPS = float(os.getenv("TARGET_RPS", "108.333333"))
WAIT_MIN = float(os.getenv("WAIT_MIN", "0.01"))
WAIT_MAX = float(os.getenv("WAIT_MAX", "0.05"))

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
