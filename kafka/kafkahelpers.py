from confluent_kafka import Consumer
from .aioproducer import AIOProducer
from flatbufferhelpers import create_f142_message
import uuid


def create_producer() -> AIOProducer:
    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "message.max.bytes": "20000000",
    }
    return AIOProducer(producer_config)


def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": uuid.uuid4(),
            "default.topic.config": {"auto.offset.reset": "latest"},
        }
    )


def publish_f142_message(producer: AIOProducer, topic: str, value: int, kafka_timestamp: int = None):
    """
    Publish an f142 message to a given topic.
    :param topic: Name of topic to publish to
    :param kafka_timestamp: Timestamp to set in the Kafka header (milliseconds after unix epoch)
    """
    f142_message = create_f142_message(value, kafka_timestamp)
    producer.produce(topic, f142_message)
