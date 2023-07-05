import pytest

from forwarder.kafka.kafka_helpers import get_sasl_config, parse_kafka_uri


def test_no_topic_specified():
    test_uri = "SASL_PLAINTEXT+SCRAM-SHA-256\\user@localhost:9092"
    broker, topic, protocol, mechanism, username = parse_kafka_uri(test_uri)
    assert not topic
    assert broker == "localhost:9092"
    assert protocol == "SASL_PLAINTEXT"
    assert mechanism == "SCRAM-SHA-256"
    assert username == "user"


def test_uri_with_broker_name_and_topic_successfully_split():
    test_broker = "localhost"
    test_topic = "some_topic"
    test_uri = f"{test_broker}/{test_topic}"
    broker, topic, _, _, _ = parse_kafka_uri(test_uri)
    assert broker == test_broker
    assert topic == test_topic


def test_uri_with_port_after_broker_is_included_in_broker_output():
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_broker}/{test_topic}"
    broker, topic, _, _, _ = parse_kafka_uri(test_uri)
    assert broker == test_broker
    assert topic == test_topic


def test_raises_exception_if_uri_with_username_and_no_security_protocol_or_sasl_mechanism():
    test_username = "some_user"
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_username}@{test_broker}/{test_topic}"
    with pytest.raises(RuntimeError):
        parse_kafka_uri(test_uri)


def test_raises_exception_if_uri_with_username_and_no_security_protocol():
    test_sasl_mechanism = "SCRAM-SHA-256"
    test_username = "some_user"
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_sasl_mechanism}\\{test_username}@{test_broker}/{test_topic}"
    with pytest.raises(RuntimeError):
        parse_kafka_uri(test_uri)


def test_raises_exception_if_uri_with_username_and_no_sasl_mechanism():
    test_protocol = "SASL_SSL"
    test_username = "some_user"
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_protocol}+{test_username}@{test_broker}/{test_topic}"
    with pytest.raises(RuntimeError):
        parse_kafka_uri(test_uri)


def test_uri_with_security_protocol_sasl_mechanism_username_port_and_topic():
    test_protocol = "SASL_PLAINTEXT"
    test_sasl_mechanism = "PLAIN"
    test_username = "some_user"
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_protocol}+{test_sasl_mechanism}\\{test_username}@{test_broker}/{test_topic}"
    _, _, protocol, sasl_mechanism, username = parse_kafka_uri(test_uri)
    c = get_sasl_config(protocol, sasl_mechanism, username, "some_password")
    assert test_protocol == c["security.protocol"]
    assert test_sasl_mechanism == c["sasl.mechanism"]


def test_uri_with_no_broker():
    test_protocol = "SASL_PLAINTEXT"
    test_sasl_mechanism = "PLAIN"
    test_username = "some_user"
    test_broker = ""
    test_topic = "some_topic"
    test_uri = f"{test_protocol}+{test_sasl_mechanism}\\{test_username}@{test_broker}/{test_topic}"
    with pytest.raises(RuntimeError):
        parse_kafka_uri(test_uri)


def test_raises_exception_if_uri_has_unsupported_security_protocol():
    test_protocol = "xSASL_SSL"
    test_sasl_mechanism = "PLAIN"
    test_username = "some_user"
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_protocol}+{test_sasl_mechanism}\\{test_username}@{test_broker}/{test_topic}"
    broker, topic, protocol, sasl_mechanism, username = parse_kafka_uri(test_uri)
    with pytest.raises(RuntimeError):
        get_sasl_config(sasl_mechanism, username, "some_password")


def test_raises_exception_if_uri_has_unsupported_sasl_mechanism():
    test_protocol = "SASL_SSL"
    test_sasl_mechanism = "xPLAIN"
    test_username = "some_user"
    test_broker = "localhost:9092"
    test_topic = "some_topic"
    test_uri = f"{test_protocol}+{test_sasl_mechanism}\\{test_username}@{test_broker}/{test_topic}"
    broker, topic, protocol, sasl_mechanism, username = parse_kafka_uri(test_uri)
    with pytest.raises(RuntimeError):
        get_sasl_config(sasl_mechanism, username, "some_password")


def test_raises_exception_if_username_or_password_not_provided():
    """All currently supported mechanisms require username and password.
    This test may require changes if support for SASL mechanisms that do not
    require username or password is implemented.
    """
    protocol = "SASL_PLAINTEXT"
    sasl_mechanism = "PLAIN"
    with pytest.raises(RuntimeError):
        get_sasl_config(protocol, sasl_mechanism, "username", None)
    with pytest.raises(RuntimeError):
        get_sasl_config(protocol, sasl_mechanism, None, "password")
    with pytest.raises(RuntimeError):
        get_sasl_config(protocol, sasl_mechanism, None, None)
    with pytest.raises(RuntimeError):
        get_sasl_config(protocol, sasl_mechanism, "username", "")
    with pytest.raises(RuntimeError):
        get_sasl_config(protocol, sasl_mechanism, "", "password")
    with pytest.raises(RuntimeError):
        get_sasl_config(protocol, sasl_mechanism, "", "")
