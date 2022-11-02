import time
import uuid

import confluent_kafka
import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from forwarder.kafka.kafka_producer import KafkaProducer

# Use the host in the docker compose file.
# Change this if you want to run against another instance, e.g. localhost
KAFKA_HOST = "kafka1"


def _create_producer(host):
    producer_config = {
        "bootstrap.servers": f"{host}:9092",
        "message.max.bytes": "20000000",
    }
    return KafkaProducer(confluent_kafka.Producer(producer_config))


def create_consumer(host):
    consumer_config = {
        "bootstrap.servers": f"{host}:9092",
        "group.id": uuid.uuid4(),
        "auto.offset.reset": "latest",
    }

    return confluent_kafka.Consumer(consumer_config)


def assign_topic(consumer, topic):
    metadata = consumer.list_topics(timeout=10)
    if topic not in metadata.topics:
        raise Exception("Topic does not exist")

    topic_partitions = [
        confluent_kafka.TopicPartition(topic, p)
        for p in metadata.topics[topic].partitions
    ]
    consumer.assign(topic_partitions)

    # Move to last message
    for tp in topic_partitions:
        _, high = consumer.get_watermark_offsets(tp, timeout=1, cached=False)
        tp.offset = high

        # If the topic has just been assigned then seek might not be possible
        # if the assignment hasn't finished.
        # So just retry until it succeeds.
        seek_done = False
        start_time = time.monotonic()
        while not seek_done:
            if time.monotonic() > start_time + 3:
                raise RuntimeError("timed out when trying to seek topic end")
            try:
                consumer.seek(tp)
                seek_done = True
            except KafkaException:
                time.sleep(0.5)


def create_topic(host, name):
    admin_client = AdminClient({"bootstrap.servers": f"{host}:9092"})
    topic = NewTopic(name, 1, 1)
    future = admin_client.create_topics([topic])
    while not future[name].done():
        pass


class TestKafkaContract:
    @pytest.fixture(autouse=True)
    def prepare(self):
        self.consumer = create_consumer(KAFKA_HOST)
        self.producer = _create_producer(KAFKA_HOST)
        yield
        self.consumer.close()
        self.producer.close()

    def test_write_and_read_message(self):
        topic = "test_write_and_read_message"
        create_topic(KAFKA_HOST, topic)
        assign_topic(self.consumer, topic)

        message = f"hello {uuid.uuid4()}"
        self.producer.produce(topic, message.encode(), int(time.time() * 1000))

        start_time = time.monotonic()
        while not (msg := self.consumer.poll(timeout=0.5)):
            if time.monotonic() > start_time + 5:
                assert False, "timed out for some reason"
            time.sleep(0.1)

        assert msg.value() == message.encode()
