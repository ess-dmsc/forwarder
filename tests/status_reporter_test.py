from forwarder.status_reporter import StatusReporter
from typing import Dict
from tests.kafka.fake_producer import FakeProducer
import json


def test_when_update_handlers_exist_their_channel_names_are_reported_in_status():
    test_channel_name_1 = "test_channel_name_1"
    test_channel_name_2 = "test_channel_name_2"

    # Normally the values in this dictionary are the update handler objects
    # but the StatusReporter only uses the keys
    update_handlers = {test_channel_name_1: 1, test_channel_name_2: 2}

    fake_producer = FakeProducer()
    status_reporter = StatusReporter(update_handlers, fake_producer, "status_topic")  # type: ignore
    status_reporter.report_status()

    if fake_producer.published_payload is not None:
        produced_status_message = json.loads(fake_producer.published_payload)
    # Using set comprehension as order is unimportant
    assert {
        stream["channel_name"] for stream in produced_status_message["streams"]
    } == {
        test_channel_name_1,
        test_channel_name_2,
    }, "Expected channel names for existing update handlers to be reported in the status message"


def test_when_no_update_handlers_exist_no_streams_are_present_in_reported_status():
    update_handlers: Dict = {}

    fake_producer = FakeProducer()
    status_reporter = StatusReporter(update_handlers, fake_producer, "status_topic")  # type: ignore
    status_reporter.report_status()

    if fake_producer.published_payload is not None:
        produced_status_message = json.loads(fake_producer.published_payload)
    assert (
        len(produced_status_message["streams"]) == 0
    ), "Expected no streams in reported status message as there are no update handlers"
