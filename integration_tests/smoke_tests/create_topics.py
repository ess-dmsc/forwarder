import os
import sys

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
)
print(sys.path)

from integration_tests.contract_tests.test_kafka_contract import create_topic

# Use the host in the docker compose file.
# Change this if you want to run against another instance, e.g. localhost
KAFKA_HOST = "kafka1"

CONFIG_TOPIC = "forwarder_commands"
STATUS_TOPIC = "forwarder_status"
STORAGE_TOPIC = "forwarder_storage"
DATA_TOPIC = "forwarder_data"


def create_topics():
    create_topic(KAFKA_HOST, CONFIG_TOPIC)
    create_topic(KAFKA_HOST, STATUS_TOPIC)
    create_topic(KAFKA_HOST, STORAGE_TOPIC)
    create_topic(KAFKA_HOST, DATA_TOPIC)


if __name__ == "__main__":
    create_topics()
