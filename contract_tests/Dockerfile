FROM ubuntu:jammy

USER root

RUN apt-get update && apt-get install -yq python3 python3-pip python3.10-venv
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN apt-get install -yq git build-essential
RUN git clone https://github.com/ess-dmsc/forwarder.git && cd forwarder && git checkout contract_tests
RUN cd forwarder && python -m pip install -r requirements-dev.txt

