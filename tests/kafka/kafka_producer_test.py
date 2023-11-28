from contextlib import closing

from confluent_kafka import KafkaError

from forwarder.kafka.kafka_producer import KafkaProducer
from forwarder.metrics import Counter, Summary


def test_producer_increments_counter_on_message():
    class FakeProducer:
        def produce(self, topic, payload, key, on_delivery, timestamp):
            on_delivery(None, "IGNORED")

        def flush(self, _):
            pass

        def poll(self, _):
            pass

    update_msg_counter = Counter(
        "successful_sends_total", "Total number of updates sent to kafka"
    )

    with closing(KafkaProducer(FakeProducer(), update_msg_counter)) as kafka_producer:
        kafka_producer.produce(
            "IRRELEVANT_TOPIC", b"IRRELEVANT_PAYLOAD", 0, key="PV_NAME"
        )

    assert update_msg_counter.value == 1


def test_producer_increments_buffer_error_counter_on_buffer_error():
    class FakeProducer:
        def produce(self, topic, payload, key, on_delivery, timestamp):
            raise BufferError

        def flush(self, _):
            pass

        def poll(self, _):
            pass

    update_buffer_err_counter = Counter(
        "send_buffer_errors_total", "Kafka producer queue errors"
    )

    with closing(
        KafkaProducer(
            FakeProducer(), update_buffer_err_counter=update_buffer_err_counter
        )
    ) as kafka_producer:
        kafka_producer.produce(
            "IRRELEVANT_TOPIC", b"IRRELEVANT_PAYLOAD", 0, key="PV_NAME"
        )

    assert update_buffer_err_counter.value == 1


def test_producer_increments_delivery_error_counter_on_delivery_error():
    class FakeProducer:
        def produce(self, topic, payload, key, on_delivery, timestamp):
            error = KafkaError(KafkaError.INVALID_CONFIG)
            on_delivery(error, "some error message")

        def flush(self, _):
            pass

        def poll(self, _):
            pass

    update_delivery_err_counter = Counter(
        "send_delivery_errors_total", "Kafka delivery errors"
    )

    with closing(
        KafkaProducer(
            FakeProducer(), update_delivery_err_counter=update_delivery_err_counter
        )
    ) as kafka_producer:
        kafka_producer.produce(
            "IRRELEVANT_TOPIC", b"IRRELEVANT_PAYLOAD", 0, key="PV_NAME"
        )

    assert update_delivery_err_counter.value == 1


def test_producer_increments_latency_metric_on_message_delivery():
    class FakeMessage:
        def latency(self):
            return 123.123

    class FakeProducer:
        def produce(self, topic, payload, key, on_delivery, timestamp):
            on_delivery(None, FakeMessage())

        def flush(self, _):
            pass

        def poll(self, _):
            pass

    latency_metric = Summary("latency_seconds", "latency")

    with closing(
        KafkaProducer(FakeProducer(), latency_metric=latency_metric)
    ) as kafka_producer:
        kafka_producer.produce(
            "IRRELEVANT_TOPIC", b"IRRELEVANT_PAYLOAD", 0, key="PV_NAME"
        )

    assert latency_metric.sum == 123.123
