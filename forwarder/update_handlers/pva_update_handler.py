from collections import namedtuple
from p4p.client.thread import Context as PVAContext
from p4p import Value
from forwarder.kafka.kafka_producer import KafkaProducer
from forwarder.application_logger import get_logger
from typing import Optional, Tuple
from threading import Lock, Event
from forwarder.update_handlers.schema_publishers import schema_publishers
from forwarder.repeat_timer import RepeatTimer, milliseconds_to_seconds
from forwarder.epics_to_serialisable_types import (
    numpy_type_from_channel_type,
    caproto_alarm_severity_to_f142,
    caproto_alarm_status_to_f142,
)
import numpy as np
from p4p.nt.enum import ntenum


CachedValue = namedtuple("CachedValue", ["value", "status", "severity", "timestamp"])


class PVAUpdateHandler:
    """
    Monitors via EPICS v4 Process Variable Access (PVA),
    serialises updates in FlatBuffers and passes them onto an Kafka Producer.
    PVA support from p4p library.
    """

    def __init__(
        self,
        producer: KafkaProducer,
        context: PVAContext,
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

    def _monitor_callback(self, response: Value):
        if self._output_type is None:
            data_type = self.__get_type(response)
            try:
                self._output_type = self.__adapt_to_type(data_type)
            except KeyError:
                self._logger.error(
                    f"Don't know what numpy dtype to use for channel type {data_type}"
                )

        timestamp = self.__get_timestamp(response)

        severity, status, value = self.__get_values(response)
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
        self._sub.close()

    def __get_values(self, response):
        value = np.array(self._get_value(response))
        status = response.raw.alarm.status
        severity = response.raw.alarm.severity
        return severity, status, value

    def __subscribe(self, context, pv_name):
        self._sub = context.monitor(pv_name, self._monitor_callback)

    def __get_timestamp(self, response):
        return (
            response.raw.timeStamp.secondsPastEpoch * 1_000_000_000
        ) + response.raw.timeStamp.nanoseconds

    def __get_type(self, response):
        return type(response)

    def __get_status(self, response):
        return response.raw.alarm.status

    def __adapt_to_type(self, data_type):
        data_type = numpy_type_from_channel_type[data_type]
        if data_type is ntenum:
            self._get_value = lambda resp: resp.raw.value.index
        else:
            self._get_value = lambda resp: resp.raw.value
        return data_type
