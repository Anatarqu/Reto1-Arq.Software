FROM python:3.12-slim
WORKDIR /app
COPY dispatcher.py idempotency.py .
RUN pip install --no-cache-dir fastapi uvicorn pika redis cachetools
CMD ["uvicorn","dispatcher:app","--host","0.0.0.0","--port","8090","--workers","1"]
