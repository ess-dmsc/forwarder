# Stage 1: Build & test
FROM python:3.9 AS test

ARG HTTP_PROXY=""
ARG HTTPS_PROXY=""
ARG NO_PROXY=""

ENV http_proxy="$HTTP_PROXY"
ENV https_proxy="$HTTPS_PROXY"
ENV no_proxy="$NO_PROXY"

WORKDIR /usr/local/src/forwarder
COPY . .
RUN pip install -r requirements.txt -r requirements-dev.txt

# Stage 2: Release image
FROM python:3.9-slim AS release

ARG HTTP_PROXY=""
ARG HTTPS_PROXY=""
ARG NO_PROXY=""

ENV http_proxy="$HTTP_PROXY"
ENV https_proxy="$HTTPS_PROXY"
ENV no_proxy="$NO_PROXY"

WORKDIR /usr/local/src/forwarder
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /etc/forwarder

ENTRYPOINT ["python", "forwarder_launch.py"]
CMD ["--config-file", "/etc/forwarder/config.ini"]
