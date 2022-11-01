# Docker image for running python-based tests
FROM ubuntu:jammy

USER root

RUN apt-get update && apt-get install -yq python3 python3-pip python3-venv vim git
RUN ln -s /usr/bin/python3 /usr/bin/python
