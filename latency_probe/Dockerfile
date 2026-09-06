FROM python:3.12-slim
WORKDIR /app
COPY ws_latency_probe.py .
RUN pip install --no-cache-dir websockets
CMD ["python","ws_latency_probe.py"]
