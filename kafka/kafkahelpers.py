from confluent_kafka import Consumer
from .aioproducer import AIOProducer
from streaming_data_types.logdata_f142 import serialise_f142
import uuid
import numpy as np

BROKER_ADDRESS = "localhost:9092"


def _millseconds_to_nanoseconds(time_ms):
    return int(time_ms * 1000000)


def create_producer() -> AIOProducer:
    producer_config = {
        "bootstrap.servers": BROKER_ADDRESS,
        "message.max.bytes": "20000000",
    }
    return AIOProducer(producer_config)


def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": BROKER_ADDRESS,
            "group.id": uuid.uuid4(),
            "default.topic.config": {"auto.offset.reset": "latest"},
        }
    )


def publish_f142_message(
    producer: AIOProducer, topic: str, data: np.array, kafka_timestamp: int = None,
):
    """
    Publish an f142 message to a given topic.
    :param topic: Name of topic to publish to
    :param kafka_timestamp: Timestamp to set in the Kafka header (milliseconds after unix epoch)
    """
    f142_message = serialise_f142(
        data, "Forwarder-Python", _millseconds_to_nanoseconds(kafka_timestamp)
    )
    producer.produce(topic, f142_message)
