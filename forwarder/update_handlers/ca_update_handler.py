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
        (self._pv,) = context.get_pvs(pv_name)
        # Subscribe with "data_type='time'" to get timestamp and alarm fields
        sub = self._pv.subscribe(data_type="time")
        sub.add_callback(self._monitor_callback)

        self._cached_update: Optional[Tuple[ReadNotifyResponse, int]] = None
        self._output_type = None
        self._repeating_timer = None
        self._cache_lock = Lock()

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
            try:
                self._output_type = numpy_type_from_channel_type[response.data_type]
            except KeyError:
                self._logger.error(
                    f"Don't know what numpy dtype to use for channel type {ChannelType(response.data_type)}"
                )

        with self._cache_lock:
            timestamp = _seconds_to_nanoseconds(response.metadata.timestamp)
            # If this is the first update or the alarm status has changed, then
            # include alarm status in message
            if (
                self._cached_update is None
                or response.metadata.status != self._cached_update[0].metadata.status
            ):
                self._message_publisher(
                    self._producer,
                    self._output_topic,
                    np.squeeze(response.data).astype(self._output_type),
                    self._pv.name,
                    timestamp,
                    caproto_alarm_status_to_f142[response.metadata.status],
                    caproto_alarm_severity_to_f142[response.metadata.severity],
                )
            else:
                # Otherwise FlatBuffers will use the default alarm status of "NO_CHANGE"
                self._message_publisher(
                    self._producer,
                    self._output_topic,
                    np.squeeze(response.data).astype(self._output_type),
                    self._pv.name,
                    timestamp,
                )
            self._cached_update = (response, timestamp)

    def publish_cached_update(self):
        with self._cache_lock:
            if self._cached_update is not None:
                # Always include current alarm status in periodic update messages
                self._message_publisher(
                    self._producer,
                    self._output_topic,
                    np.squeeze(self._cached_update[0].data).astype(self._output_type),
                    self._pv.name,
                    self._cached_update[1],
                    caproto_alarm_status_to_f142[
                        self._cached_update[0].metadata.status
                    ],
                    caproto_alarm_severity_to_f142[
                        self._cached_update[0].metadata.severity
                    ],
                )

    def stop(self):
        """
        Stop periodic updates and unsubscribe from PV
        """
        if self._repeating_timer is not None:
            self._repeating_timer.cancel()
        self._pv.unsubscribe_all()
