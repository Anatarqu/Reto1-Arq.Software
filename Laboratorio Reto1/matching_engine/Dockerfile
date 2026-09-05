FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pika
COPY engine.py .
CMD ["python", "engine.py"]