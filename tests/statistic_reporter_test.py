import logging
from typing import Dict
from unittest.mock import ANY, MagicMock, call

from confluent_kafka import KafkaError

from forwarder.kafka.kafka_producer import KafkaProducer
from forwarder.statistics_reporter import StatisticsReporter
from forwarder.utils import Counter

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
buffer_err_counter = Counter()


def test_that_warning_logged_on_send_exception(caplog):
    update_handler: Dict = {}
    statistics_reporter = StatisticsReporter(
        "localhost", update_handler, Counter(), buffer_err_counter, Counter(), logger
    )
    statistics_reporter._sender = MagicMock()
    statistics_reporter._sender.send.side_effect = ValueError

    with caplog.at_level(logging.WARNING):
        statistics_reporter.send_statistics()

    assert caplog.text != ""


def test_statistic_reporter_sends_number_pvs():
    update_msg_counter: Counter = Counter()
    # This dictionary is of type Dict[Channel, UpdateHandler]
    # StatisticReporter only uses len of this dictionary
    update_handler = {"key1": "value1", "key2": "value2"}
    statistics_reporter = StatisticsReporter("localhost", update_handler, update_msg_counter, buffer_err_counter, Counter(), logger)  # type: ignore
    statistics_reporter._sender = MagicMock()

    statistics_reporter.send_statistics()

    calls = [
        call("number_pvs", len(update_handler.keys()), ANY),
    ]
    statistics_reporter._sender.send.assert_has_calls(calls, any_order=True)


def test_statistic_reporter_sends_total_updates():
    update_msg_counter: Counter = Counter()
    statistics_reporter = StatisticsReporter(
        "localhost", {}, update_msg_counter, buffer_err_counter, Counter(), logger
    )
    statistics_reporter._sender = MagicMock()

    update_msg_counter.increment()
    update_msg_counter.increment()
    update_msg_counter.increment()
    statistics_reporter.send_statistics()

    calls = [
        call("total_updates", 3, ANY),
    ]
    statistics_reporter._sender.send.assert_has_calls(calls, any_order=True)


def test_producer_increments_counter_on_message():
    class FakeProducer:
        def produce(self, topic, payload, key, on_delivery, timestamp):
            on_delivery(None, "IGNORED")

        def flush(self, _):
            pass

        def poll(self, _):
            pass

    update_msg_counter: Counter = Counter()
    kafka_producer = KafkaProducer(FakeProducer(), update_msg_counter)

    kafka_producer.produce("IRRELEVANT_TOPIC", b"IRRELEVANT_PAYLOAD", 0, key="PV_NAME")
    kafka_producer.close()
    assert update_msg_counter.value == 1


def test_producer_increments_buffer_error_counter_on_buffer_error():
    class FakeProducer:
        def produce(self, topic, payload, key, on_delivery, timestamp):
            raise BufferError

        def flush(self, _):
            pass

        def poll(self, _):
            pass

    update_buffer_err_counter: Counter = Counter()
    kafka_producer = KafkaProducer(
        FakeProducer(), update_buffer_err_counter=update_buffer_err_counter
    )

    kafka_producer.produce("IRRELEVANT_TOPIC", b"IRRELEVANT_PAYLOAD", 0, key="PV_NAME")
    kafka_producer.close()
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

    update_delivery_err_counter: Counter = Counter()
    kafka_producer = KafkaProducer(
        FakeProducer(), update_delivery_err_counter=update_delivery_err_counter
    )

    kafka_producer.produce("IRRELEVANT_TOPIC", b"IRRELEVANT_PAYLOAD", 0, key="PV_NAME")
    kafka_producer.close()
    assert update_delivery_err_counter.value == 1


def test_statistic_reporter_sends_data_loss_errors():
    update_buffer_err_counter: Counter = Counter()
    statistics_reporter = StatisticsReporter(
        "localhost", {}, Counter(), update_buffer_err_counter, Counter(), logger
    )
    statistics_reporter._sender = MagicMock()

    update_buffer_err_counter.increment()
    update_buffer_err_counter.increment()
    update_buffer_err_counter.increment()
    statistics_reporter.send_statistics()

    calls = [
        call("data_loss_errors", 3, ANY),
    ]
    statistics_reporter._sender.send.assert_has_calls(calls, any_order=True)


def test_statistic_reporter_sends_delivery_errors():
    update_delivery_err_counter: Counter = Counter()
    statistics_reporter = StatisticsReporter(
        "localhost", {}, Counter(), Counter(), update_delivery_err_counter, logger
    )
    statistics_reporter._sender = MagicMock()

    update_delivery_err_counter.increment()
    update_delivery_err_counter.increment()
    update_delivery_err_counter.increment()
    statistics_reporter.send_statistics()

    calls = [
        call("kafka_delivery_errors", 3, ANY),
    ]
    statistics_reporter._sender.send.assert_has_calls(calls, any_order=True)
