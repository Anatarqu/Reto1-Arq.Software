FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi uvicorn pika
COPY gateway.py .
CMD ["uvicorn", "gateway:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]