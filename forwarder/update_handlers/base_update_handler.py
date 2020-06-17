from collections import namedtuple
from forwarder.application_logger import get_logger
from forwarder.kafka.kafka_producer import KafkaProducer
from caproto import ReadNotifyResponse
from p4p import Value
from p4p.client.thread import Context as PVAContext
import numpy as np
from threading import Lock
from forwarder.repeat_timer import RepeatTimer, milliseconds_to_seconds
from forwarder.epics_to_serialisable_types import (
    caproto_alarm_severity_to_f142,
    caproto_alarm_status_to_f142,
)
from caproto.threading.client import Context as CAContext
from typing import Optional, Tuple
from forwarder.update_handlers.schema_publishers import schema_publishers


CachedValue = namedtuple("CachedValue", ["value", "status", "severity", "timestamp"])


class BaseUpdateHandler:
    def __init__(
        self,
        producer: KafkaProducer,
        context: [CAContext, PVAContext],
        pv_name: str,
        output_topic: str,
        schema: str,
        periodic_update_ms: Optional[int] = None,
    ):
        self._logger = get_logger()
        self._producer = producer
        self._output_topic = output_topic

        self._subscribe(context, pv_name)
        self._pv_name = pv_name

        self._output_type = None
        self._repeating_timer = None
        self._cache_lock = Lock()
        self._cached_update: Optional[CachedValue] = None

        try:
            self._message_publisher = schema_publishers[schema]
        except KeyError:
            raise ValueError(
                f"{schema} is not a recognised supported schema, use one of "
                f"{list(schema_publishers.keys())}"
            )

        if periodic_update_ms is not None:
            self._repeating_timer = RepeatTimer(
                milliseconds_to_seconds(periodic_update_ms), self.publish_cached_update
            )
            self._repeating_timer.start()

    def _monitor_callback(self, response: [ReadNotifyResponse, Value]):
        if self._output_type is None:
            try:
                self._output_type = self._get_epics_type(response)
            except KeyError:
                return

        timestamp = self._get_timestamp(response)

        value, status, severity = self._get_values(response)
        with self._cache_lock:
            # If this is the first update or the alarm status has changed, then
            # include alarm status in message
            if self._cached_update is None or status != self._cached_update.status:
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
                # Otherwise FlatBuffers will use the default alarm status of
                # "NO_CHANGE"
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
                # Always include current alarm status in periodic update
                # messages
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
        self._unsubscribe()

    def _unsubscribe(self):
        raise NotImplementedError("Not implemented")

    def _get_values(self, response):
        raise NotImplementedError("Not implemented")

    def _subscribe(self, context, pv_name):
        raise NotImplementedError("Not implemented")

    def _get_timestamp(self, response):
        raise NotImplementedError("Not implemented")

    def _get_epics_type(self, response):
        raise NotImplementedError("Not implemented")
