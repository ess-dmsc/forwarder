import numpy as np
import pytest
from streaming_data_types.timestamps_tdct import deserialise_tdct

from forwarder.kafka.kafka_helpers import (
    get_broker_and_topic_from_uri,
    publish_tdct_message,
)
from tests.kafka.fake_producer import FakeProducer


def test_raises_exception_if_no_forward_slash_present():
    test_uri = "no slash in string"
    with pytest.raises(RuntimeError):
        get_broker_and_topic_from_uri(test_uri)


def test_uri_with_broker_name_and_topic_successfully_split():
    test_broker = "localhost"
    test_topic = "some_topic"
    test_uri = f"{test_broker}/{test_topic}"
    broker, topic = get_broker_and_topic_from_uri(test_uri)
    assert broker == test_broker
    assert topic == test_topic


def test_uri_with_double_slash_in_from_of_broker_is_ok():
    test_broker = "localhost"
    test_topic = "some_topic"
    test_uri = f"//{test_broker}/{test_topic}"
    broker, topic = get_broker_and_topic_from_uri(test_uri)
    assert broker == test_broker
    assert topic == test_topic


def test_uri_with_port_after_broker_is_included_in_broker_output():
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_broker}/{test_topic}"
    broker, topic = get_broker_and_topic_from_uri(test_uri)
    assert broker == test_broker
    assert topic == test_topic


def test_tdct_publisher_converts_relative_timestamp_to_absolute():
    producer = FakeProducer()
    # These are the values that would be in the array in the PV update
    input_relative_timestamps = np.array([1, 2, 3]).astype(np.uint32)
    # This is the timestamp of the PV update
    reference_timestamp = 10
    publish_tdct_message(
        producer,  # type: ignore
        "test_topic",
        input_relative_timestamps,
        "test_source",
        reference_timestamp,
    )

    assert producer.published_payload is not None
    published_data = deserialise_tdct(producer.published_payload)
    assert np.array_equal(
        published_data.timestamps, input_relative_timestamps + reference_timestamp
    ), "Expected the relative timestamps from the EPICS update to have been converted to unix timestamps"


def test_tdct_publisher_handles_negative_relative_timestamps():
    producer = FakeProducer()
    # These are the values that would be in the array in the PV update
    input_relative_timestamps = np.array([-3, -2, -1]).astype(np.uint32)
    # This is the timestamp of the PV update
    reference_timestamp = 10
    publish_tdct_message(
        producer,  # type: ignore
        "test_topic",
        input_relative_timestamps,
        "test_source",
        reference_timestamp,
    )

    assert producer.published_payload is not None
    published_data = deserialise_tdct(producer.published_payload)
    assert np.array_equal(
        published_data.timestamps, input_relative_timestamps + reference_timestamp
    ), "Expected the relative timestamps from the EPICS update to have been converted to unix timestamps"


def test_tdct_publisher_publishes_successfully_when_there_is_only_a_single_chopper_timestamp():
    producer = FakeProducer()
    # These are the values that would be in the array in the PV update
    input_relative_timestamps = np.array(1).astype(np.uint32)
    # This is the timestamp of the PV update
    reference_timestamp = 10
    publish_tdct_message(
        producer,  # type: ignore
        "test_topic",
        input_relative_timestamps,
        "test_source",
        reference_timestamp,
    )

    assert producer.published_payload is not None
    published_data = deserialise_tdct(producer.published_payload)
    assert np.array_equal(
        published_data.timestamps,
        np.atleast_1d(input_relative_timestamps + reference_timestamp),
    ), "Expected the relative timestamps from the EPICS update to have been converted to unix timestamps"
