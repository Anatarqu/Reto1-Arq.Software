from locust import HttpUser, task, between
import uuid
import random
import time
import os

class FinancialTrader(HttpUser):
    wait_time = between(0.01, 0.05) # Permite ejecutar alta concurrencia por segundo

    def on_start(self):
        self.symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        self.trader_id = f"TRADER_{random.randint(1, 1000)}"

    @task
    def post_order(self):
        # Determinación de lado basada en cuotas (800 BUYS vs 500 SELLS de Fase 1)
        side = 'BUY' if random.random() < (800 / 1300) else 'SELL'

        payload = {
            "order_id": str(uuid.uuid4()),
            "trader_id": self.trader_id,
            "symbol": random.choice(self.symbols),
            "side": side,
            "price": round(random.uniform(100.0, 1500.0), 2),
            "quantity": random.randint(10, 200),
            "timestamp": int(time.time() * 1000)
        }

        headers = {'Content-Type': 'application/json'}

        # Envío directo al CQRS Ingestion Endpoint (API Gateway)
        self.client.post("/api/v1/orders", json=payload, headers=headers)