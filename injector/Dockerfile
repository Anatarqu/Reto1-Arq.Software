FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir locust
COPY locustfile.py .
# Interfaz web manual en el puerto 8089: tú escribes el Host, el número de
# usuarios y el spawn rate ahí, y controlas Start/Stop desde el navegador.
# El wait_time del locustfile igual se recalcula solo según cuántos usuarios
# pongas, así que puedes cambiar la cantidad en cualquier momento del
# experimento sin que el cálculo de tasa objetivo se rompa.
CMD ["locust", "-f", "locustfile.py"]
