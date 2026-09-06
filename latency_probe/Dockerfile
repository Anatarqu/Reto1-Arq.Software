FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir websockets
COPY ws_latency_probe.py .
CMD ["python", "ws_latency_probe.py"]
