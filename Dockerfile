# Stage 1: Build & test
FROM python:3.9 AS test

WORKDIR /usr/local/src/forwarder
COPY . .
RUN pip install -r requirements.txt -r requirements-dev.txt

# Stage 2: Release image
FROM python:3.9-slim AS release

WORKDIR /usr/local/src/forwarder
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /etc/forwarder

ENTRYPOINT ["python", "forwarder_launch.py"]
CMD ["--config-file", "/etc/forwarder/config.ini"]
