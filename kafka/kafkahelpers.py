from confluent_kafka import Consumer
from .aioproducer import AIOProducer
from flatbufferhelpers import create_f142_message
import uuid


def create_producer():
    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "message.max.bytes": "20000000",
    }

    return AIOProducer(**producer_config)


def create_consumer():
    return Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": uuid.uuid4(),
            "default.topic.config": {"auto.offset.reset": "latest"},
        }
    )


def delivery_callback(err, msg):
    if err:
        print(f'%% Message failed delivery: {err}')
    else:
        print('%% Message delivered to %s [%d] @ %d\n' %
              (msg.topic(), msg.partition(), msg.offset()))


def publish_f142_message(producer, topic, value, kafka_timestamp=None):
    """
    Publish an f142 message to a given topic.
    Optionally set the timestamp in the kafka header to allow, for example, fake "historical" data.
    :param topic: Name of topic to publish to
    :param kafka_timestamp: Timestamp to set in the Kafka header (milliseconds after unix epoch)
    """
    f142_message = create_f142_message(value, kafka_timestamp)
    print("serialised message")

    try:
        producer.produce(topic, f142_message, timestamp=kafka_timestamp, callback=delivery_callback)
    except BufferError:
        print(f'%% Local producer queue is full ({len(producer)} messages awaiting delivery): try again')

    print("published message")
    producer.poll(0)
