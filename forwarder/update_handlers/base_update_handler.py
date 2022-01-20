from datetime import datetime, timedelta, timezone
from typing import Optional, Union

from forwarder.application_logger import get_logger
from forwarder.kafka.kafka_helpers import _nanoseconds_to_milliseconds
from forwarder.kafka.kafka_producer import KafkaProducer

LOWER_AGE_LIMIT = timedelta(days=365.25)
UPPER_AGE_LIMIT = timedelta(minutes=10)


class BaseUpdateHandler:
    def __init__(self, producer: KafkaProducer, pv_name: str, output_topic: str):
        self._logger = get_logger()
        self._producer = producer
        self._output_topic = output_topic
        self._pv_name = pv_name
        self._last_timestamp = datetime(
            year=1900, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc
        )

    def _publish_message(
        self, message: Optional[bytes], timestamp_ns: Union[int, float]
    ) -> bool:
        if message is None:
            self._logger.error(
                f'Rejecting update from PV "{self._pv_name}" as the message was not serialised.'
            )
            return False
        message_datetime = datetime.fromtimestamp(timestamp_ns / 1e9, tz=timezone.utc)
        if message_datetime < self._last_timestamp:
            self._logger.error(
                f'Rejecting update from PV "{self._pv_name}" as its timestamp is older than the previous message timestamp from that PV ({message_datetime} vs {self._last_timestamp}).'
            )
            return False
        current_datetime = datetime.now(tz=timezone.utc)
        if message_datetime < current_datetime - LOWER_AGE_LIMIT:
            self._logger.error(
                f'Rejecting update from PV "{self._pv_name}" as its timestamp is older than allowed ({LOWER_AGE_LIMIT}).'
            )
            return False
        if message_datetime > current_datetime + UPPER_AGE_LIMIT:
            self._logger.error(
                f'Rejecting update from PV "{self._pv_name}" as its timestamp is from further into the future than allowed ({UPPER_AGE_LIMIT}).'
            )
            return False
        self._last_timestamp = message_datetime
        self._producer.produce(
            self._output_topic,
            message,
            _nanoseconds_to_milliseconds(int(timestamp_ns)),
            key=self._pv_name,
        )
        return True
