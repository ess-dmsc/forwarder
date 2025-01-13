FROM python:3

WORKDIR /usr/src/app

COPY pyproject.toml ./

RUN pip install --no-cache-dir .

# Expose ports for kafka, EPICS CA, EPICS PVA
EXPOSE 9092 5064 5065 5075 5076

COPY . .

ENTRYPOINT  ["python", "./forwarder_launch.py"]
