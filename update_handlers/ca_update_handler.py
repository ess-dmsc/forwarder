from application_logger import get_logger
from kafka.kafka_producer import KafkaProducer
from caproto import ReadNotifyResponse, ChannelType
import numpy as np
from threading import Lock, Event
from repeat_timer import RepeatTimer, milliseconds_to_seconds
from epics_to_serialisable_types import (
    numpy_type_from_channel_type,
    caproto_alarm_severity_to_f142,
    caproto_alarm_status_to_f142,
)
from caproto.threading.client import Context as CAContext
import time
from typing import Optional
from update_handlers.schema_publishers import schema_publishers


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
        schema: str = "f142",
        periodic_update_ms: Optional[int] = None,
    ):
        self._logger = get_logger()
        self._producer = producer
        self._output_topic = output_topic
        (self._pv,) = context.get_pvs(pv_name)
        # Prevent our monitor timing out if PV is not available right now
        self._pv.timeout = None
        # Subscribe with "data_type='control'" otherwise we don't get the metadata with alarm fields
        sub = self._pv.subscribe(data_type="control")
        sub.add_callback(self._monitor_callback)

        self._cached_update = None
        self._output_type = None
        self._stop_timer_flag = Event()
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
        # Create timestamp as early as possible
        timestamp = time.time_ns()
        if self._output_type is None:
            try:
                self._output_type = numpy_type_from_channel_type[response.data_type]
            except KeyError:
                self._logger.error(
                    f"Don't know what numpy dtype to use for channel type {ChannelType(response.data_type)}"
                )

        with self._cache_lock:
            # If this is the first update or the alarm status has changed, then include alarm status in message
            if (
                self._cached_update is None
                or response.metadata.status != self._cached_update[0].metadata.status
            ):
                self._message_publisher(
                    self._producer,
                    self._output_topic,
                    np.squeeze(response.data).astype(self._output_type),
                    source_name=self._pv.name,
                    timestamp_ns=timestamp,
                    alarm_status=caproto_alarm_status_to_f142[response.metadata.status],
                    alarm_severity=caproto_alarm_severity_to_f142[
                        response.metadata.severity
                    ],
                )
            else:
                # Otherwise FlatBuffers will use the default alarm status of "NO_CHANGE"
                self._message_publisher(
                    self._producer,
                    self._output_topic,
                    np.squeeze(response.data).astype(self._output_type),
                    source_name=self._pv.name,
                    timestamp_ns=timestamp,
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
                    source_name=self._pv.name,
                    timestamp_ns=self._cached_update[1],
                    alarm_status=caproto_alarm_status_to_f142[
                        self._cached_update[0].metadata.status
                    ],
                    alarm_severity=caproto_alarm_severity_to_f142[
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
