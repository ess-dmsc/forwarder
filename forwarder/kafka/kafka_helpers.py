import uuid
from typing import Dict, Optional, Tuple, Union

from confluent_kafka import Consumer, Producer
from streaming_data_types.epics_connection_info_ep00 import serialise_ep00
from streaming_data_types.fbschemas.epics_connection_info_ep00.EventType import (
    EventType as ConnectionStatusEventType,
)

from forwarder.utils import Counter

from .kafka_producer import KafkaProducer

DEFAULT_SASL_MECHANISM = "SCRAM-SHA-256"


def get_sasl_config(
    mechanism: str, username: Optional[str] = None, password: Optional[str] = None
) -> dict:
    """Return a dict with SASL configuration parameters.
    Supported protocols: SASL_PLAINTEXT (i.e. without TLS)
    Supported mechanisms: PLAIN, SCRAM-SHA-512, SCRAM-SHA-256.
    Note that whereas some SASL mechanisms do not require user/password, the three
    we currently support do.
    """
    supported_sasl_mechanisms = ["PLAIN", "SCRAM-SHA-512", "SCRAM-SHA-256"]
    if mechanism not in supported_sasl_mechanisms:
        raise RuntimeError(
            f"SASL mechanism {mechanism} not supported, use one of {supported_sasl_mechanisms}"
        )
    if not username or not password:
        raise RuntimeError(
            f"Username and password must be provided to use SASL {mechanism}"
        )
    sasl_config = {
        "sasl.mechanism": mechanism,
        "security.protocol": "SASL_PLAINTEXT",  # SASL_PLAINTEXT for plaintext, SASL_SSL for encrypted
        "sasl.username": username,
        "sasl.password": password,
    }
    return sasl_config


def create_producer(
    broker_address: str,
    sasl_mechanism: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    counter: Optional[Counter] = None,
    buffer_err_counter: Optional[Counter] = None,
) -> KafkaProducer:
    producer_config = {
        "bootstrap.servers": broker_address,
        "message.max.bytes": "20000000",
    }
    if sasl_mechanism:
        producer_config.update(get_sasl_config(sasl_mechanism, username, password))
    producer = Producer(producer_config)
    return KafkaProducer(
        producer,
        update_msg_counter=counter,
        update_buffer_err_counter=buffer_err_counter,
    )


def create_consumer(
    broker_address: str,
    sasl_mechanism: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> Consumer:
    consumer_config = {
        "bootstrap.servers": broker_address,
        "group.id": uuid.uuid4(),
        "default.topic.config": {"auto.offset.reset": "latest"},
    }
    if sasl_mechanism:
        consumer_config.update(get_sasl_config(sasl_mechanism, username, password))
    return Consumer(consumer_config)


def parse_kafka_uri(uri: str, is_topic_required=True) -> Tuple[str, str, str, str]:
    topic = uri.split("/")[-1]
    if is_topic_required and ("/" not in uri or not topic):
        raise RuntimeError(
            f"Unable to parse URI {uri}, should be of form [[SASL_MECHANISM\\]username@]localhost:9092/topic"
        )
    broker_and_username = "".join(uri.split("/")[:-1])
    broker, sasl_mechanism, username = _get_broker_and_username_from_uri(
        broker_and_username
    )
    return broker, topic, sasl_mechanism, username


def _get_broker_and_username_from_uri(uri: str) -> Tuple[str, str, str]:
    if "/" in uri:
        raise RuntimeError(
            f"Unable to parse URI {uri}, should be of form [[SASL_MECHANISM\\]username@]localhost:9092"
        )
    username = "".join(uri.split("@")[:-1]).split("\\")[-1]
    sasl_mechanism = "".join("".join(uri.split("@")[:-1]).split("\\")[:-1])
    if not sasl_mechanism:
        sasl_mechanism = DEFAULT_SASL_MECHANISM
    broker = uri.split("@")[-1]
    broker = broker.strip("/")
    return broker, sasl_mechanism, username


def _nanoseconds_to_milliseconds(time_ns: int) -> int:
    return int(time_ns) // 1_000_000


_state_str_to_enum: Dict[Union[str, Exception], ConnectionStatusEventType] = {
    "connected": ConnectionStatusEventType.CONNECTED,
    "disconnected": ConnectionStatusEventType.DISCONNECTED,
    "destroyed": ConnectionStatusEventType.DESTROYED,
}


def publish_connection_status_message(
    producer: KafkaProducer, topic: str, pv_name: str, timestamp_ns: int, state: str
):
    producer.produce(
        topic,
        serialise_ep00(
            timestamp_ns,
            _state_str_to_enum.get(state, ConnectionStatusEventType.UNKNOWN),
            pv_name,
        ),
        timestamp_ms=_nanoseconds_to_milliseconds(timestamp_ns),
    )


def seconds_to_nanoseconds(time_seconds: float) -> int:
    return int(time_seconds * 1_000_000_000)
