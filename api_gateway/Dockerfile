FROM python:3.12-slim
WORKDIR /app
COPY gateway.py .
RUN pip install --no-cache-dir fastapi uvicorn pika pydantic
CMD ["uvicorn","gateway:app","--host","0.0.0.0","--port","8000","--workers","2"]
