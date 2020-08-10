from forwarder.kafka.kafka_helpers import get_broker_and_topic_from_uri
from forwarder.kafka.kafka_helpers import publish_tdct_message
from tests.kafka.fake_producer import FakeProducer
from streaming_data_types.timestamps_tdct import deserialise_tdct
import numpy as np
import pytest


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


def test_tdct_publisher_converts_relative_timestamp_converted_to_absolute():
    producer = FakeProducer()
    # These are the values that would be in the array in the PV update,
    # I assume here that they are in nanoseconds
    input_relative_timestamps = np.array([1, 2, 3]).astype(np.uint32)
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
    )
