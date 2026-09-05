FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir locust
COPY locustfile.py .
# Inicia la interfaz web en el puerto 8089
CMD ["locust", "-f", "locustfile.py"]