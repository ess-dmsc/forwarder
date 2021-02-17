from confluent_kafka import Consumer
from .kafka_producer import KafkaProducer
from streaming_data_types.logdata_f142 import serialise_f142
from streaming_data_types.timestamps_tdct import serialise_tdct
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import AlarmSeverity
import uuid
import numpy as np
from typing import Optional, Tuple, Dict, Union
from streaming_data_types.fbschemas.epics_connection_info_ep00.EventType import (
    EventType as ConnectionStatusEventType,
)
from streaming_data_types.epics_connection_info_ep00 import serialise_ep00
from forwarder.utils import Counter


def create_producer(
    broker_address: str,
    counter: Optional[Counter] = None,
) -> KafkaProducer:
    producer_config = {
        "bootstrap.servers": broker_address,
        "message.max.bytes": "20000000",
    }
    return KafkaProducer(producer_config, update_msg_counter=counter)


def create_consumer(broker_address: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": broker_address,
            "group.id": uuid.uuid4(),
            "default.topic.config": {"auto.offset.reset": "latest"},
        }
    )


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
    return time_ns // 1_000_000


def publish_f142_message(
    producer: KafkaProducer,
    topic: str,
    data: np.ndarray,
    source_name: str,
    timestamp_ns: int,
    alarm_status: Optional[AlarmStatus] = None,
    alarm_severity: Optional[AlarmSeverity] = None,
):
    """
    Publish an f142 message to a given topic.
    :param producer: Kafka producer to publish update with
    :param topic: Name of topic to publish to
    :param data: Value of the PV update
    :param source_name: Name of the PV
    :param timestamp_ns: Timestamp for value (nanoseconds after unix epoch)
    :param alarm_status:
    :param alarm_severity:
    """
    if alarm_status is None:
        f142_message = serialise_f142(
            value=data,
            source_name=source_name,
            timestamp_unix_ns=timestamp_ns,
        )
    else:
        f142_message = serialise_f142(
            value=data,
            source_name=source_name,
            timestamp_unix_ns=timestamp_ns,
            alarm_status=alarm_status,
            alarm_severity=alarm_severity,
        )
    producer.produce(
        topic,
        f142_message,
        key=source_name,
        timestamp_ms=_nanoseconds_to_milliseconds(timestamp_ns),
    )


def publish_tdct_message(
    producer: KafkaProducer,
    topic: str,
    data: np.ndarray,
    source_name: str,
    timestamp_ns: int,
    *unused,
):
    """
    Publish an tdct message to a given topic.
    Currently the tdct does not contain alarms, but if it turns out to be the long term
    solution for getting chopper timestamps into Kafka we will likely add alarms to the
    schema

    :param producer: Kafka producer to publish update with
    :param topic: Name of topic to publish to
    :param data: Value of the PV update
    :param source_name: Name of the PV
    :param timestamp_ns: Timestamp for value (nanoseconds after unix epoch)
    :param unused: Allow other args to be passed to match signature of other
        publish_*_message functions
    """
    # Timestamps in the data array are nanoseconds relative to the EPICS update timestamp
    # Convert to absolute (relative to unix epoch)
    unix_epoch_timestamps_ns = data + timestamp_ns
    producer.produce(
        topic,
        serialise_tdct(
            name=source_name, timestamps=unix_epoch_timestamps_ns.astype(np.uint64)
        ),
        key=source_name,
        timestamp_ms=_nanoseconds_to_milliseconds(timestamp_ns),
    )


_state_str_to_enum: Dict[Union[str, Exception], ConnectionStatusEventType] = {
    "connected": ConnectionStatusEventType.CONNECTED,
    "disconnected": ConnectionStatusEventType.DISCONNECTED,
    "destroyed": ConnectionStatusEventType.DESTROYED,
}


def publish_connection_status_message(
    producer: KafkaProducer,
    topic: str,
    pv_name: str,
    timestamp_ns: int,
    state: str,
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
