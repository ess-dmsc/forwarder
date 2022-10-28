FROM ubuntu:jammy

USER root

RUN apt-get update && apt-get install -yq python3 python3-pip
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN python -m pip install p4p caproto pytest
RUN apt-get install -yq git
RUN git clone https://github.com/ess-dmsc/forwarder.git && cd forwarder && git checkout contract_tests

