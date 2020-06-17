from collections import namedtuple
from forwarder.application_logger import get_logger
from forwarder.kafka.kafka_producer import KafkaProducer
from caproto import ReadNotifyResponse, ChannelType
import numpy as np
from threading import Lock
from forwarder.repeat_timer import RepeatTimer, milliseconds_to_seconds
from forwarder.epics_to_serialisable_types import (
    numpy_type_from_channel_type,
    caproto_alarm_severity_to_f142,
    caproto_alarm_status_to_f142,
)
from caproto.threading.client import Context as CAContext
from typing import Optional, Tuple
from forwarder.update_handlers.schema_publishers import schema_publishers


def _seconds_to_nanoseconds(time_seconds: float) -> int:
    return int(time_seconds * 1_000_000_000)


CachedValue = namedtuple("CachedValue", ["value", "status", "severity", "timestamp"])


class CAUpdateHandler:
    """
    Monitors via EPICS v3 Channel Access (CA),
    serialises updates in FlatBuffers and passes them onto an Kafka Producer.
    CA support from caproto library.
    """

    def __init__(
        self,
        producer: KafkaProducer,
        context: CAContext,
        pv_name: str,
        output_topic: str,
        schema: str,
        periodic_update_ms: Optional[int] = None,
    ):
        self._logger = get_logger()
        self._producer = producer
        self._output_topic = output_topic

        self.__subscribe(context, pv_name)
        self._pv_name = pv_name

        self._output_type = None
        self._repeating_timer = None
        self._cache_lock = Lock()
        self._cached_update: Optional[CachedValue] = None

        try:
            self._message_publisher = schema_publishers[schema]
        except KeyError:
            raise ValueError(
                f"{schema} is not a recognised supported schema, use one of {list(schema_publishers.keys())}"
            )

        if periodic_update_ms is not None:
            self._repeating_timer = RepeatTimer(
                milliseconds_to_seconds(periodic_update_ms), self.publish_cached_update
            )
            self._repeating_timer.start()

    def _monitor_callback(self, sub, response: ReadNotifyResponse):
        if self._output_type is None:
            data_type = self.__get_type(response)
            try:
                self._output_type = self.__adapt_to_type(data_type)
            except KeyError:
                self._logger.error(
                    f"Don't know what numpy dtype to use for channel type {ChannelType(data_type)}"
                )

        timestamp = self.__get_timestamp(response)

        value, status, severity = self.__get_values(response)
        with self._cache_lock:
            # If this is the first update or the alarm status has changed, then
            # include alarm status in message
            if (
                    self._cached_update is None
                    or status != self._cached_update.status
            ):
                self._message_publisher(
                    self._producer,
                    self._output_topic,
                    np.squeeze(value).astype(self._output_type),
                    self._pv_name,
                    timestamp,
                    caproto_alarm_status_to_f142[status],
                    caproto_alarm_severity_to_f142[severity],
                )
            else:
                # Otherwise FlatBuffers will use the default alarm status of "NO_CHANGE"
                self._message_publisher(
                    self._producer,
                    self._output_topic,
                    np.squeeze(value).astype(self._output_type),
                    self._pv_name,
                    timestamp,
                )
            self._cached_update = CachedValue(value, status, severity, timestamp)

    def publish_cached_update(self):
        with self._cache_lock:
            if self._cached_update is not None:
                # Always include current alarm status in periodic update messages
                self._message_publisher(
                    self._producer,
                    self._output_topic,
                    np.squeeze(self._cached_update.value).astype(self._output_type),
                    self._pv_name,
                    self._cached_update.timestamp,
                    caproto_alarm_status_to_f142[self._cached_update.status],
                    caproto_alarm_severity_to_f142[self._cached_update.severity],
                )

    def stop(self):
        """
        Stop periodic updates and unsubscribe from PV
        """
        if self._repeating_timer is not None:
            self._repeating_timer.cancel()
        self.__unsubscribe()

    def __unsubscribe(self):
        self._pv.unsubscribe_all()

    def __get_values(self, response):
        value = response.data
        status = response.metadata.status
        severity = response.metadata.severity
        return value, status, severity

    def __subscribe(self, context, pv_name):
        (self._pv,) = context.get_pvs(pv_name)
        # Prevent our monitor timing out if PV is not available right now
        # This causes subscribe() to block so commenting out for now,
        # see ticket #10.
        # self._pv.timeout = None
        # Subscribe with "data_type='time'" to get timestamp and alarm fields
        sub = self._pv.subscribe(data_type="time")
        sub.add_callback(self._monitor_callback)

    def __get_timestamp(self, response):
        return _seconds_to_nanoseconds(response.metadata.timestamp)

    def __get_type(self, response):
        return response.data_type

    def __adapt_to_type(self, data_type):
        return numpy_type_from_channel_type[data_type]

    def __get_status(self, response):
        return response.metadata.status
