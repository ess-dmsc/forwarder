from p4p.client.thread import Context as PVAContext
from p4p import Value
from kafka.kafka_producer import KafkaProducer
from application_logger import get_logger
from typing import Optional
from threading import Lock, Event
from update_handlers.schema_publishers import schema_publishers
from repeat_timer import RepeatTimer, milliseconds_to_seconds
from epics_to_serialisable_types import (
    numpy_type_from_channel_type,
    caproto_alarm_severity_to_f142,
    caproto_alarm_status_to_f142,
)
import numpy as np
from p4p.nt.enum import ntenum


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
        schema: str = "f142",
        periodic_update_ms: Optional[int] = None,
    ):
        self._logger = get_logger()
        self._producer = producer
        self._output_topic = output_topic

        self._sub = context.monitor(pv_name, self._monitor_callback)
        self._pv_name = pv_name

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

    def _monitor_callback(self, response: Value):
        timestamp = (
            response.raw.timeStamp.secondsPastEpoch * 1_000_000_000
        ) + response.raw.timeStamp.nanoseconds
        if self._output_type is None:
            try:
                self._output_type = numpy_type_from_channel_type[type(response)]
                if type(response) is ntenum:
                    self._get_value = lambda resp: resp.raw.value.index
                else:
                    self._get_value = lambda resp: resp.raw.value
            except KeyError:
                self._logger.error(
                    f"Don't know what numpy dtype to use for channel type {type(response)}"
                )

        with self._cache_lock:
            # If this is the first update or the alarm status has changed, then include alarm status in message
            if (
                self._cached_update is None
                or response.raw.alarm.status != self._cached_update[0].raw.alarm.status
            ):
                self._message_publisher(
                    self._producer,
                    self._output_topic,
                    np.squeeze(np.array(self._get_value(response))).astype(
                        self._output_type
                    ),
                    source_name=self._pv_name,
                    timestamp_ns=timestamp,
                    alarm_status=caproto_alarm_status_to_f142[
                        response.raw.alarm.status
                    ],
                    alarm_severity=caproto_alarm_severity_to_f142[
                        response.raw.alarm.severity
                    ],
                )
            else:
                self._message_publisher(
                    self._producer,
                    self._output_topic,
                    np.squeeze(np.array(self._get_value(response))).astype(
                        self._output_type
                    ),
                    source_name=self._pv_name,
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
                    np.squeeze(
                        np.array(self._get_value(self._cached_update[0]))
                    ).astype(self._output_type),
                    source_name=self._pv_name,
                    timestamp_ns=self._cached_update[1],
                    alarm_status=caproto_alarm_status_to_f142[
                        self._cached_update[0].raw.alarm.status
                    ],
                    alarm_severity=caproto_alarm_severity_to_f142[
                        self._cached_update[0].raw.alarm.severity
                    ],
                )

    def stop(self):
        """
        Stop periodic updates and unsubscribe from PV
        """
        if self._repeating_timer is not None:
            self._repeating_timer.cancel()
        self._sub.close()
