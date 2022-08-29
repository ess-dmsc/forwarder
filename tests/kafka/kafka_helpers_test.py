import pytest

from forwarder.kafka.kafka_helpers import (
    get_broker_and_username_from_uri,
    get_broker_topic_and_username_from_uri,
)


def test_raises_exception_if_no_forward_slash_present():
    test_uri = "no slash in string"
    with pytest.raises(RuntimeError):
        get_broker_topic_and_username_from_uri(test_uri)


def test_uri_with_broker_name_and_topic_successfully_split():
    test_broker = "localhost"
    test_topic = "some_topic"
    test_uri = f"{test_broker}/{test_topic}"
    broker, topic, _ = get_broker_topic_and_username_from_uri(test_uri)
    assert broker == test_broker
    assert topic == test_topic


def test_uri_with_double_slash_in_from_of_broker_is_ok():
    test_broker = "localhost"
    test_topic = "some_topic"
    test_uri = f"//{test_broker}/{test_topic}"
    broker, topic, _ = get_broker_topic_and_username_from_uri(test_uri)
    assert broker == test_broker
    assert topic == test_topic


def test_uri_with_port_after_broker_is_included_in_broker_output():
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_broker}/{test_topic}"
    broker, topic, _ = get_broker_topic_and_username_from_uri(test_uri)
    assert broker == test_broker
    assert topic == test_topic


def test_raises_exception_if_broker_only_uri_contains_slash():
    test_username = "some_user"
    test_broker = "localhost:9092"
    test_uri = f"{test_username}@{test_broker}/"
    with pytest.raises(RuntimeError):
        get_broker_and_username_from_uri(test_uri)


def test_uri_with_username_port_and_topic():
    test_username = "some_user"
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_username}@{test_broker}/{test_topic}"
    broker, topic, username = get_broker_topic_and_username_from_uri(test_uri)
    assert username == test_username
    assert broker == test_broker
    assert topic == test_topic
