import pytest
from forwarder.kafka.kafka_helpers import get_broker_and_topic_from_uri


def test_broker_and_topic_parsed():
    uri = "localhost:9092/some_topic"
    broker, topic = get_broker_and_topic_from_uri(uri)

    assert broker == "localhost:9092"
    assert topic == "some_topic"


def test_no_broker_throws_when_strict():
    uri = "some_topic"

    with pytest.raises(RuntimeError):
        get_broker_and_topic_from_uri(uri, broker_required=True)


def test_no_broker_but_leading_slash_throws_when_strict():
    uri = "/some_topic"

    with pytest.raises(RuntimeError):
        get_broker_and_topic_from_uri(uri, broker_required=True)


def test_no_topic_but_trailing_slash_throws_when_strict():
    uri = "some_host:9092/"

    with pytest.raises(RuntimeError):
        get_broker_and_topic_from_uri(uri, broker_required=True)


def test_no_broker_returns_topic_only_when_not_strict():
    uri = "some_topic"

    broker, topic = get_broker_and_topic_from_uri(uri, broker_required=False)

    assert broker == ""
    assert topic == "some_topic"


def test_double_slash_before_broker_is_okay():
    uri = "//localhost:9092/some_topic"

    broker, topic = get_broker_and_topic_from_uri(uri, broker_required=True)

    assert broker == "localhost:9092"
    assert topic == "some_topic"
