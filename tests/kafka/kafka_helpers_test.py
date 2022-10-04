import pytest

from forwarder.kafka.kafka_helpers import (
    get_broker_topic_and_username_from_uri,
    get_sasl_config,
)


def test_null_topic_if_not_present():
    test_uri = "SCRAM-SHA-256\\user@localhost:9092"
    broker, topic, mechanism, username = get_broker_topic_and_username_from_uri(
        test_uri
    )
    assert not topic
    assert broker == "localhost:9092"
    assert mechanism == "SCRAM-SHA-256"
    assert username == "user"


def test_uri_with_broker_name_and_topic_successfully_split():
    test_broker = "localhost"
    test_topic = "some_topic"
    test_uri = f"{test_broker}/{test_topic}"
    broker, topic, _, username = get_broker_topic_and_username_from_uri(test_uri)
    assert broker == test_broker
    assert topic == test_topic
    assert not username


def test_uri_with_port_after_broker_is_included_in_broker_output():
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_broker}/{test_topic}"
    broker, topic, _, username = get_broker_topic_and_username_from_uri(test_uri)
    assert broker == test_broker
    assert topic == test_topic
    assert not username


def test_raises_exception_if_broker_only_uri_contains_slash():
    test_username = "some_user"
    test_broker = "localhost:9092"
    test_uri = f"{test_username}@{test_broker}/"
    with pytest.raises(RuntimeError):
        get_broker_topic_and_username_from_uri(test_uri)


def test_raises_exception_if_uri_with_username_and_no_sasl_mechanism():
    test_username = "some_user"
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_username}@{test_broker}/{test_topic}"
    with pytest.raises(RuntimeError):
        get_broker_topic_and_username_from_uri(test_uri)


def test_uri_with_sasl_mechanism_username_port_and_topic():
    test_sasl_mechanism = "PLAIN"
    test_username = "some_user"
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_sasl_mechanism}\\{test_username}@{test_broker}/{test_topic}"
    broker, topic, sasl_mechanism, username = get_broker_topic_and_username_from_uri(
        test_uri
    )
    assert sasl_mechanism == test_sasl_mechanism
    assert username == test_username
    assert broker == test_broker
    assert topic == test_topic
    assert (
        test_sasl_mechanism
        == get_sasl_config(sasl_mechanism, username, "some_password")["sasl.mechanism"]
    )


def test_uri_with_no_broker():
    test_sasl_mechanism = "PLAIN"
    test_username = "some_user"
    test_broker = ""
    test_topic = "some_topic"
    test_uri = f"{test_sasl_mechanism}\\{test_username}@{test_broker}/{test_topic}"
    with pytest.raises(RuntimeError):
        get_broker_topic_and_username_from_uri(test_uri)


def test_raises_exception_if_uri_has_unsupported_sasl_mechanism():
    test_sasl_mechanism = "xPLAIN"
    test_username = "some_user"
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_sasl_mechanism}\\{test_username}@{test_broker}/{test_topic}"
    broker, topic, sasl_mechanism, username = get_broker_topic_and_username_from_uri(
        test_uri
    )
    with pytest.raises(RuntimeError):
        get_sasl_config(sasl_mechanism, username, "some_password")


def test_raises_exception_if_username_or_password_not_provided():
    """All currently supported mechanisms require username and password.
    This test may require changes if support for SASL mechanisms that do not
    require username or password is implemented.
    """
    sasl_mechanism = "PLAIN"
    with pytest.raises(RuntimeError):
        get_sasl_config(sasl_mechanism, "username", None)
    with pytest.raises(RuntimeError):
        get_sasl_config(sasl_mechanism, None, "password")
    with pytest.raises(RuntimeError):
        get_sasl_config(sasl_mechanism, None, None)
    with pytest.raises(RuntimeError):
        get_sasl_config(sasl_mechanism, "username", "")
    with pytest.raises(RuntimeError):
        get_sasl_config(sasl_mechanism, "", "password")
    with pytest.raises(RuntimeError):
        get_sasl_config(sasl_mechanism, "", "")
