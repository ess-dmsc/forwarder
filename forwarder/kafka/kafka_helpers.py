import uuid
from typing import Dict, Optional, Tuple, Union

from confluent_kafka import Consumer, Producer
from streaming_data_types.epics_connection_info_ep00 import serialise_ep00
from streaming_data_types.fbschemas.epics_connection_info_ep00.EventType import (
    EventType as ConnectionStatusEventType,
)

from forwarder.utils import Counter

from .kafka_producer import KafkaProducer


def sasl_conf():
    """Return a dict with SASL configuration parameters.
    TODO: This is an example, we must load the configuration from elsewhere!
    """
    sasl_conf = {
        "sasl.mechanism": "PLAIN",  # GSSAPI, PLAIN, SCRAM-SHA-512, SCRAM-SHA-256
        "security.protocol": "SASL_PLAINTEXT",  # SASL_PLAINTEXT, SASL_SSL
    }
    # Example for PLAIN:
    sasl_conf.update(
        {
            "sasl.username": "client",
            "sasl.password": "client-secret",
        }
    )
    return sasl_conf


def create_producer(
    broker_address: str,
    counter: Optional[Counter] = None,
    buffer_err_counter: Optional[Counter] = None,
) -> KafkaProducer:
    producer_config = {
        "bootstrap.servers": broker_address,
        "message.max.bytes": "20000000",
    }
    producer_config.update(sasl_conf())
    producer = Producer(producer_config)
    return KafkaProducer(
        producer,
        update_msg_counter=counter,
        update_buffer_err_counter=buffer_err_counter,
    )


def create_consumer(broker_address: str) -> Consumer:
    consumer_config = {
        "bootstrap.servers": broker_address,
        "group.id": uuid.uuid4(),
        "default.topic.config": {"auto.offset.reset": "latest"},
    }
    consumer_config.update(sasl_conf())
    return Consumer(consumer_config)


def get_broker_and_topic_from_uri(uri: str) -> Tuple[str, str]:
    if "/" not in uri:
        raise RuntimeError(
            f"Unable to parse URI {uri}, should be of form localhost:9092/topic"
        )
    topic = uri.split("/")[-1]
    broker = "".join(uri.split("/")[:-1])
    broker = broker.strip("/")
    return broker, topic


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
