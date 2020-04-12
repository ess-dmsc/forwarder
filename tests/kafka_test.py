from kafka.kafka_helpers import get_broker_and_topic_from_uri
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
