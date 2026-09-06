import os
import time
import uuid
import random

from locust import HttpUser, task


# ---------------------------------------------------------------------------
# Objetivos de tasa TOTAL (agregada) tal como los define el PDF del reto.
# No son proporcionales entre sí (6.500/1.300 = 5x, pero 250/40 usuarios = 6.25x),
# así que un único "wait_time" fijo no puede servir para ambas fases: se
# recalcula el ritmo por usuario en cada request, en función de cuántos
# usuarios están realmente activos en ese instante — sin importar qué número
# hayas puesto tú manualmente en la interfaz web de Locust.
#
# Todo esto es configurable por variable de entorno si en el experimento
# necesitas otros objetivos de tasa o otro umbral de usuarios, sin tocar
# código.
# ---------------------------------------------------------------------------
FASE1_TARGET_RPS = float(os.getenv("FASE1_TARGET_RPS", 1300 / 60))   # ~21.67 req/s
FASE2_TARGET_RPS = float(os.getenv("FASE2_TARGET_RPS", 6500 / 60))   # ~108.33 req/s

# Umbral de usuarios que separa "línea base" de "pico" para elegir qué tasa
# objetivo aplicar. Por defecto coincide con el tope de usuarios de Fase 1
# del PDF (40), pero es ajustable si decides usar otros valores en la prueba.
FASE1_USER_THRESHOLD = int(os.getenv("FASE1_USER_THRESHOLD", "40"))


class FinancialTrader(HttpUser):

    def wait_time(self):
        """
        Período ALEATORIO entre órdenes (proceso de Poisson / distribución
        exponencial), no un intervalo fijo. Cada trader es una fuente de
        llegadas independiente y sin memoria: la probabilidad de que mande
        la próxima orden en el siguiente instante no depende de cuánto hace
        que mandó la anterior. Es el modelo estándar de teoría de colas para
        llegadas de órdenes independientes, y es justamente lo que la
        arquitectura está diseñada para amortiguar (ráfagas reales, no una
        cadencia artificialmente uniforme).

        La MEDIA de esa distribución exponencial es la que se recalcula en
        cada llamada según cuántos usuarios están activos ahora mismo, para
        que el promedio agregado converja a FASE1_TARGET_RPS o
        FASE2_TARGET_RPS según corresponda — el período de cada orden
        individual es aleatorio, pero el total del sistema sigue apuntando
        al objetivo de la fase vigente.
        """
        runner = self.environment.runner
        current_users = runner.user_count if runner and runner.user_count else 1

        target_total_rps = (
            FASE1_TARGET_RPS
            if current_users <= FASE1_USER_THRESHOLD
            else FASE2_TARGET_RPS
        )
        per_user_rps = target_total_rps / current_users

        # Muestra de una distribución exponencial con media 1/per_user_rps.
        return random.expovariate(per_user_rps)

    def on_start(self):
        self.trader_id = f"TRADER_{random.randint(1, 1000)}"
        self.symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

    @task
    def post_order(self):
        side = "BUY" if random.random() < (800 / 1300) else "SELL"

        payload = {
            "order_id": str(uuid.uuid4()),
            "trader_id": self.trader_id,
            "symbol": random.choice(self.symbols),
            "side": side,
            "price": round(random.uniform(100.0, 1500.0), 2),
            "quantity": random.randint(10, 200),
            "timestamp": int(time.time() * 1000),
        }

        with self.client.post(
            "/api/v1/orders",
            json=payload,
            headers={"Content-Type": "application/json"},
            name="POST /api/v1/orders",
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f"HTTP {response.status_code}")
            else:
                try:
                    data = response.json()
                    if data.get("status") != "ACK":
                        response.failure("Gateway no devolvió ACK")
                except Exception:
                    response.failure("Respuesta JSON inválida")
