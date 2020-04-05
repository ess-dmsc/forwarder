from confluent_kafka import Consumer
from .aio_producer import AIOProducer
from streaming_data_types.logdata_f142 import serialise_f142
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import AlarmSeverity
import uuid
import numpy as np
from typing import Optional

BROKER_ADDRESS = "localhost:9092"


def _millseconds_to_nanoseconds(time_ms):
    return int(time_ms * 1000000)


def create_producer() -> AIOProducer:
    producer_config = {
        "bootstrap.servers": BROKER_ADDRESS,
        "message.max.bytes": "20000000",
    }
    return AIOProducer(producer_config)


def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": BROKER_ADDRESS,
            "group.id": uuid.uuid4(),
            "default.topic.config": {"auto.offset.reset": "latest"},
        }
    )


def publish_f142_message(
    producer: AIOProducer,
    topic: str,
    data: np.array,
    kafka_timestamp: int,
    source_name: str,
    alarm_status: Optional[AlarmStatus] = None,
    alarm_severity: Optional[AlarmSeverity] = None,
):
    """
    Publish an f142 message to a given topic.
    :param producer: Kafka producer to publish update with
    :param topic: Name of topic to publish to
    :param data: Value of the PV update
    :param kafka_timestamp: Timestamp to set in the Kafka header (milliseconds after unix epoch)
    :param source_name: Name of the PV
    :param alarm_status:
    :param alarm_severity:
    """
    if alarm_status is None:
        f142_message = serialise_f142(
            value=data,
            source_name=source_name,
            timestamp_unix_ns=_millseconds_to_nanoseconds(kafka_timestamp),
        )
    else:
        f142_message = serialise_f142(
            value=data,
            source_name=source_name,
            timestamp_unix_ns=_millseconds_to_nanoseconds(kafka_timestamp),
            alarm_status=alarm_status,
            alarm_severity=alarm_severity,
        )
    producer.produce(topic, f142_message)
