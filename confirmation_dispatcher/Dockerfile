FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" pika redis cachetools
COPY idempotency.py dispatcher.py .
CMD ["uvicorn", "dispatcher:app", "--host", "0.0.0.0", "--port", "8090"]
