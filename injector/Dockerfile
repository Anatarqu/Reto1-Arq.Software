FROM locustio/locust:2.41.2
WORKDIR /mnt/locust
COPY locustfile.py .
CMD ["-f","locustfile.py","--host=http://api-gateway:8000","--web-host=0.0.0.0","--web-port=8089"]
