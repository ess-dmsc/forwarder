from confluent_kafka import Consumer
from .aio_producer import AIOProducer
from streaming_data_types.logdata_f142 import serialise_f142
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import AlarmSeverity
import uuid
import numpy as np
from typing import Optional, Tuple

OUTPUT_BROKER_ADDRESS = "localhost:9092"


def create_producer() -> AIOProducer:
    producer_config = {
        "bootstrap.servers": OUTPUT_BROKER_ADDRESS,
        "message.max.bytes": "20000000",
    }
    return AIOProducer(producer_config)


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


def publish_f142_message(
    producer: AIOProducer,
    topic: str,
    data: np.array,
    timestamp_ns: int,
    source_name: str,
    alarm_status: Optional[AlarmStatus] = None,
    alarm_severity: Optional[AlarmSeverity] = None,
):
    """
    Publish an f142 message to a given topic.
    :param producer: Kafka producer to publish update with
    :param topic: Name of topic to publish to
    :param data: Value of the PV update
    :param timestamp_ns: Timestamp for value (nanoseconds after unix epoch)
    :param source_name: Name of the PV
    :param alarm_status:
    :param alarm_severity:
    """
    if alarm_status is None:
        f142_message = serialise_f142(
            value=data, source_name=source_name, timestamp_unix_ns=timestamp_ns,
        )
    else:
        f142_message = serialise_f142(
            value=data,
            source_name=source_name,
            timestamp_unix_ns=timestamp_ns,
            alarm_status=alarm_status,
            alarm_severity=alarm_severity,
        )
    producer.produce(topic, f142_message)
