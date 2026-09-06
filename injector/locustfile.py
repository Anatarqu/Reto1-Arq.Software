```python
from locust import HttpUser, task, between

import uuid
import random
import time


class FinancialTrader(HttpUser):

    wait_time = between(0.01, 0.05)


    def on_start(self):

        # Locust asigna un trader estable a cada usuario.
        #
        # __generation_id__ permite que cada usuario tenga
        # un identificador diferente durante la prueba.

        self.trader_id = (
            f"TRADER_{id(self)}"
        )

        self.symbols = [
            "AAPL",
            "MSFT",
            "GOOGL",
            "AMZN",
            "TSLA",
        ]


    @task
    def post_order(self):

        side = (
            "BUY"
            if random.random() < (800 / 1300)
            else "SELL"
        )


        payload = {

            "order_id":
                str(uuid.uuid4()),

            "trader_id":
                self.trader_id,

            "symbol":
                random.choice(self.symbols),

            "side":
                side,

            "price":
                round(
                    random.uniform(
                        100.0,
                        1500.0
                    ),
                    2,
                ),

            "quantity":
                random.randint(
                    10,
                    200,
                ),

            "timestamp":
                int(
                    time.time() * 1000
                ),
        }


        with self.client.post(
            "/api/v1/orders",
            json=payload,
            headers={
                "Content-Type":
                    "application/json"
            },
            name="POST /api/v1/orders",
            catch_response=True,
        ) as response:

            if response.status_code != 200:

                response.failure(
                    f"HTTP {response.status_code}"
                )

            else:

                try:

                    data = response.json()

                    if data.get("status") != "ACK":

                        response.failure(
                            "Gateway no devolvió ACK"
                        )

                except Exception:

                    response.failure(
                        "Respuesta JSON inválida"
                    )
```
