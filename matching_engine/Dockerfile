FROM python:3.12-slim
WORKDIR /app
COPY engine.py .
RUN pip install --no-cache-dir pika
CMD ["python","engine.py"]
