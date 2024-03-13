import uuid
from typing import Dict, Optional, Tuple, Union

from confluent_kafka import Consumer, Producer
from streaming_data_types.epics_connection_ep01 import ConnectionInfo, serialise_ep01

from forwarder.metrics import Counter, Summary
from forwarder.metrics.statistics_reporter import StatisticsReporter

from .kafka_producer import KafkaProducer


def get_sasl_config(
    protocol: str,
    mechanism: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> dict:
    """Return a dict with SASL configuration parameters.
    Supported protocols: SASL_PLAINTEXT (i.e. without TLS) and SASL_SSL
    Supported mechanisms: PLAIN, SCRAM-SHA-512, SCRAM-SHA-256.
    Note that whereas some SASL mechanisms do not require user/password, the three
    we currently support do.
    """
    supported_security_protocols = ["SASL_PLAINTEXT", "SASL_SSL"]
    supported_sasl_mechanisms = ["PLAIN", "SCRAM-SHA-512", "SCRAM-SHA-256"]

    if protocol not in supported_security_protocols:
        raise RuntimeError(
            f"Security protocol {protocol} not supported, use one of {supported_security_protocols}"
        )

    if not mechanism:
        raise RuntimeError(
            f"SASL mechanism must be specified for security protocol {protocol}"
        )
    elif mechanism not in supported_sasl_mechanisms:
        raise RuntimeError(
            f"SASL mechanism {mechanism} not supported, use one of {supported_sasl_mechanisms}"
        )

    if not username or not password:
        raise RuntimeError(
            f"Username and password must be provided to use SASL {mechanism}"
        )

    sasl_config = {
        "security.protocol": protocol,  # SASL_PLAINTEXT for plaintext, SASL_SSL for encrypted
        "sasl.mechanism": mechanism,
        "sasl.username": username,
        "sasl.password": password,
    }
    return sasl_config


def create_producer(
    broker_address: str,
    security_protocol: Optional[str] = None,
    sasl_mechanism: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    ssl_ca_file: Optional[str] = None,
    statistics_reporter: Optional[StatisticsReporter] = None,
) -> KafkaProducer:
    producer_config = {
        "bootstrap.servers": broker_address,
        "message.max.bytes": "20000000",
    }
    if security_protocol:
        producer_config.update(
            get_sasl_config(security_protocol, sasl_mechanism, username, password)
        )
        if ssl_ca_file:
            producer_config["ssl.ca.location"] = ssl_ca_file
    producer = Producer(producer_config)
    if not statistics_reporter:
        return KafkaProducer(producer)
    else:
        update_message_counter = Counter(
            "successful_sends_total", "Total number of updates sent to kafka"
        )
        buffer_err_counter = Counter(
            "send_buffer_errors_total", "Kafka producer queue errors"
        )
        delivery_err_counter = Counter(
            "send_delivery_errors_total", "Kafka delivery errors"
        )
        latency_metric = Summary(
            "send_latency_seconds",
            "Time from the produce call until ACK is received from the broker",
        )
        statistics_reporter.register_metric(
            update_message_counter.name, update_message_counter
        )
        statistics_reporter.register_metric(buffer_err_counter.name, buffer_err_counter)
        statistics_reporter.register_metric(
            delivery_err_counter.name, delivery_err_counter
        )
        statistics_reporter.register_metric(latency_metric.name, latency_metric)
        return KafkaProducer(
            producer,
            update_msg_counter=update_message_counter,
            update_buffer_err_counter=buffer_err_counter,
            update_delivery_err_counter=delivery_err_counter,
            latency_metric=latency_metric,
        )


def create_consumer(
    broker_address: str,
    security_protocol: Optional[str] = None,
    sasl_mechanism: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    ssl_ca_file: Optional[str] = None,
) -> Consumer:
    consumer_config = {
        "bootstrap.servers": broker_address,
        "group.id": uuid.uuid4(),
        "default.topic.config": {"auto.offset.reset": "latest"},
    }
    if security_protocol:
        consumer_config.update(
            get_sasl_config(security_protocol, sasl_mechanism, username, password)
        )
        if ssl_ca_file:
            consumer_config["ssl.ca.location"] = ssl_ca_file
    return Consumer(consumer_config)


def parse_kafka_uri(uri: str) -> Tuple[str, str, str, str, str]:
    """Parse Kafka connection URI.

    A broker hostname/ip must be present.
    If username is provided, a SASL mechanism must also be provided.
    Any other validation must be performed in the calling code.
    """
    security_protocol, tail = uri.split("+") if "+" in uri else ("", uri)
    sasl_mechanism, tail = tail.split("\\") if "\\" in tail else ("", tail)
    username, tail = tail.split("@") if "@" in tail else ("", tail)
    broker, topic = tail.split("/") if "/" in tail else (tail, "")
    if not broker:
        raise RuntimeError(
            f"Unable to parse URI {uri}, broker not defined. URI should be of form [PROTOCOL+SASL_MECHANISM\\username@]broker:9092"
        )
    if username and not (security_protocol and sasl_mechanism):
        raise RuntimeError(
            f"Unable to parse URI {uri}, PROTOCOL or SASL_MECHANISM not defined. URI should be of form [PROTOCOL+SASL_MECHANISM\\username@]broker:9092"
        )
    return broker, topic, security_protocol, sasl_mechanism, username


def _nanoseconds_to_milliseconds(time_ns: int) -> int:
    return int(time_ns) // 1_000_000


_state_str_to_enum: Dict[Union[str, Exception], ConnectionInfo] = {
    "connected": ConnectionInfo.CONNECTED,
    "disconnected": ConnectionInfo.DISCONNECTED,
    "destroyed": ConnectionInfo.DESTROYED,
    "cancelled": ConnectionInfo.CANCELLED,
    "finished": ConnectionInfo.FINISHED,
    "remote_error": ConnectionInfo.REMOTE_ERROR,
}


def publish_connection_status_message(
    producer: KafkaProducer, topic: str, pv_name: str, timestamp_ns: int, state: str
):
    producer.produce(
        topic,
        serialise_ep01(
            timestamp_ns,
            _state_str_to_enum.get(state, ConnectionInfo.UNKNOWN),
            pv_name,
        ),
        timestamp_ms=_nanoseconds_to_milliseconds(timestamp_ns),
    )


def seconds_to_nanoseconds(time_seconds: float) -> int:
    return int(time_seconds * 1_000_000_000)
