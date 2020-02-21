from confluent_kafka import Consumer
from .aioproducer import AIOProducer
from serialisation import serialise_f142
import uuid
import numpy as np
from caproto import ChannelType

BROKER_ADDRESS = "localhost:9092"


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
    producer: AIOProducer,
    topic: str,
    data: np.array,
    data_size: int,
    data_type: ChannelType,
    kafka_timestamp: int = None,
):
    """
    Publish an f142 message to a given topic.
    :param topic: Name of topic to publish to
    :param kafka_timestamp: Timestamp to set in the Kafka header (milliseconds after unix epoch)
    """
    f142_message = serialise_f142(data, data_size, data_type, kafka_timestamp)
    producer.produce(topic, f142_message)
