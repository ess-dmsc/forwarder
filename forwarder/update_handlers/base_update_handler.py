from datetime import datetime, timedelta, timezone
from typing import Optional, Union, List

from threading import Lock
from forwarder.application_logger import get_logger
from forwarder.kafka.kafka_helpers import _nanoseconds_to_milliseconds
from forwarder.kafka.kafka_producer import KafkaProducer
from forwarder.repeat_timer import RepeatTimer, milliseconds_to_seconds
from forwarder.update_handlers.schema_serialisers import schema_serialisers

LOWER_AGE_LIMIT = timedelta(days=365.25)
UPPER_AGE_LIMIT = timedelta(minutes=10)


class SerialiserTracker(object):
    def __init__(self, serialiser, publisher: "BaseUpdateHandler", periodic_update_ms: Optional[int] = None):
        self.serialiser = serialiser
        self._logger = get_logger()
        self._publisher = publisher
        self._last_timestamp = datetime(
            year=1900, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc
        )
        self._repeating_timer: Optional[RepeatTimer] = None
        if periodic_update_ms is not None:
            self._repeating_timer = RepeatTimer(
                milliseconds_to_seconds(periodic_update_ms), self._publish_cached_update
            )
            self._repeating_timer.start()
        self._cached_update: Optional[bytes] = None
        self._cached_timestamp: Union[int, float] = 0
        self._cache_lock = Lock()

    def _publish_cached_update(self):
        try:
            with self._cache_lock:
                if self._cached_update is not None:
                    self._publisher.publish_message(self._cached_update, self._cached_timestamp)
        except (RuntimeError, ValueError) as e:
            self._logger.error(
                f"Got error when publishing cached PVA update. Message was: {str(e)}"
            )

    def set_new_message(self, message: bytes, timestamp_ns: Union[int, float]):
        if message is None:
            return
        message_datetime = datetime.fromtimestamp(timestamp_ns / 1e9, tz=timezone.utc)
        if message_datetime < self._last_timestamp:
            self._logger.error(
                f'Rejecting update as its timestamp is older than the previous message timestamp from that PV ({message_datetime} vs {self._last_timestamp}).'
            )
            return
        current_datetime = datetime.now(tz=timezone.utc)
        if message_datetime < current_datetime - LOWER_AGE_LIMIT:
            self._logger.error(
                f'Rejecting update as its timestamp is older than allowed ({LOWER_AGE_LIMIT}).'
            )
            return
        if message_datetime > current_datetime + UPPER_AGE_LIMIT:
            self._logger.error(
                f'Rejecting update as its timestamp is from further into the future than allowed ({UPPER_AGE_LIMIT}).'
            )
            return
        self._last_timestamp = message_datetime
        if self._publisher.publish_message(message, timestamp_ns) and self._repeating_timer is not None:
            with self._cache_lock:
                self._cached_update = message
                self._cached_timestamp = timestamp_ns

    def stop(self):
        if self._repeating_timer is not None:
            self._repeating_timer.cancel()


class BaseUpdateHandler:
    def __init__(self, producer: KafkaProducer, pv_name: str, output_topic: str, schema: str, periodic_update_ms: Optional[int] = None):
        self._logger = get_logger()
        self._producer = producer
        self._output_topic = output_topic
        self._pv_name = pv_name
        self.serialiser_tracker_list:  List[SerialiserTracker] = []
        try:
            self.serialiser_tracker_list.append(SerialiserTracker(schema_serialisers[schema](self._pv_name), self, periodic_update_ms))
        except KeyError:
            raise ValueError(
                f"{schema} is not a recognised supported schema, use one of {list(schema_serialisers.keys())}")
        # Connection status serialiser
        self.serialiser_tracker_list.append(
            SerialiserTracker(schema_serialisers["ep00"](self._pv_name), self, periodic_update_ms))

    def publish_message(
        self, message: Optional[bytes], timestamp_ns: Union[int, float]
    ) -> bool:
        if message is None:
            self._logger.error(
                f'Rejecting update from PV "{self._pv_name}" as the message was not serialised.'
            )
            return False
        self._producer.produce(
            self._output_topic,
            message,
            _nanoseconds_to_milliseconds(int(timestamp_ns)),
            key=self._pv_name,
        )
        return True

    def stop(self):
        """
        Stop periodic updates and unsubscribe from PV
        """
        for serialiser in self.serialiser_tracker_list:
            serialiser.stop()
