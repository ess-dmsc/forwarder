from confluent_kafka import Producer, Consumer
from .flatbufferhelpers import create_f142_message
import uuid


def create_producer():
    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "message.max.bytes": "20000000",
    }
    producer = Producer(**producer_config)
    return producer


def create_consumer():
    return Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": uuid.uuid4(),
            "default.topic.config": {"auto.offset.reset": "latest"},
        }
    )


def publish_f142_message(producer, topic, kafka_timestamp=None):
    """
    Publish an f142 message to a given topic.
    Optionally set the timestamp in the kafka header to allow, for example, fake "historical" data.
    :param topic: Name of topic to publish to
    :param kafka_timestamp: Timestamp to set in the Kafka header (milliseconds after unix epoch)
    """
    f142_message = create_f142_message(kafka_timestamp)
    producer.produce(topic, f142_message, timestamp=kafka_timestamp)
    producer.poll(0)
