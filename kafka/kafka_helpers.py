from confluent_kafka import Consumer
from .aio_producer import AIOProducer
from streaming_data_types.logdata_f142 import serialise_f142
import uuid
import numpy as np
import time

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
    :param producer:
    :param topic: Name of topic to publish to
    :param data:
    :param kafka_timestamp: Timestamp to set in the Kafka header (milliseconds after unix epoch)
    """
    # TODO get timestamp from EPICS and don't allow None to this method
    if kafka_timestamp is None:
        kafka_timestamp = int(time.time() * 1000)
    f142_message = serialise_f142(
        value=data,
        source_name="Forwarder-Python",
        timestamp_unix_ns=_millseconds_to_nanoseconds(kafka_timestamp),
    )
    producer.produce(topic, f142_message)
