import os
import sys

from confluent_kafka import Producer
from streaming_data_types.fbschemas.forwarder_config_update_fc00 import (
    Protocol,
    UpdateType,
)
from streaming_data_types.forwarder_config_update_fc00 import StreamInfo, serialise_fc00

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
)

from integration_tests.contract_tests.test_kafka_contract import create_topic

# Use the host in the docker compose file.
# Change this if you want to run against another instance, e.g. localhost
KAFKA_HOST = os.getenv("FORWARDER_KAFKA_CONTAINER_NAME", "kafka1")

CONFIG_TOPIC = "forwarder_commands"
STATUS_TOPIC = "forwarder_status"
STORAGE_TOPIC = "forwarder_storage"
DATA_TOPIC = "forwarder_data"


def create_topics():
    create_topic(KAFKA_HOST, CONFIG_TOPIC)
    create_topic(KAFKA_HOST, STATUS_TOPIC)
    create_topic(KAFKA_HOST, STORAGE_TOPIC)
    create_topic(KAFKA_HOST, DATA_TOPIC)


def create_storage_item():
    streams = [
        StreamInfo("SIMPLE:DOUBLE", "f142", DATA_TOPIC, Protocol.Protocol.PVA, 1),
    ]
    producer_config = {
        "bootstrap.servers": f"{KAFKA_HOST}:9092",
        "message.max.bytes": "20000000",
    }
    producer = Producer(producer_config)
    producer.produce(STORAGE_TOPIC, serialise_fc00(UpdateType.UpdateType.ADD, streams))
    producer.flush(timeout=5)


if __name__ == "__main__":
    create_topics()
    create_storage_item()
