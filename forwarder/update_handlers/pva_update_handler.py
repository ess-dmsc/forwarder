from p4p.client.thread import Context as PVAContext
from p4p import Value
from forwarder.kafka.kafka_producer import KafkaProducer
from forwarder.application_logger import get_logger
from typing import Optional, Tuple
from threading import Lock, Event
from forwarder.update_handlers.schema_publishers import schema_publishers
from forwarder.repeat_timer import RepeatTimer, milliseconds_to_seconds
from forwarder.epics_to_serialisable_types import (
    numpy_type_from_p4p_type,
    epics_alarm_severity_to_f142,
    pva_alarm_message_to_f142_alarm_status,
)
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
import numpy as np


def _get_alarm_status(response):
    try:
        alarm_status = pva_alarm_message_to_f142_alarm_status[response.alarm.message]
    except Exception:
        alarm_status = AlarmStatus.UDF
    return alarm_status


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
        self._schema = schema

        request = context.makeRequest("field(value,timeStamp,alarm)")
        self._sub = context.monitor(pv_name, self._monitor_callback, request=request)
        self._pv_name = pv_name

        self._cached_update: Optional[Tuple[Value, int]] = None
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

    @property
    def output_topic(self):
        return self._output_topic

    @property
    def schema(self):
        return self._schema

    def _monitor_callback(self, response: Value):
        timestamp = (
            response.timeStamp.secondsPastEpoch * 1_000_000_000
        ) + response.timeStamp.nanoseconds
        if self._output_type is None:
            self._try_to_determine_type(response)

        with self._cache_lock:
            # If this is the first update or the alarm status has changed, then
            # include alarm status in message
            if (
                self._cached_update is None
                or response.alarm.message != self._cached_update[0].alarm.message
            ):
                self._message_publisher(
                    self._producer,
                    self._output_topic,
                    np.squeeze(np.array(self._get_value(response))).astype(
                        self._output_type
                    ),
                    self._pv_name,
                    timestamp,
                    _get_alarm_status(response),
                    epics_alarm_severity_to_f142[response.alarm.severity],
                )
            else:
                self._message_publisher(
                    self._producer,
                    self._output_topic,
                    np.squeeze(np.array(self._get_value(response))).astype(
                        self._output_type
                    ),
                    self._pv_name,
                    timestamp,
                )
            self._cached_update = (response, timestamp)

    def _try_to_determine_type(self, response):
        try:
            is_enum = False
            try:
                if response.type()["value"].getID() == "enum_t":
                    is_enum = True
            except AttributeError:
                # Attribute error raised because getID doesn't exist for scalar and scalar-array types.
                # Array output types are prefixed by "a", we don't need this.
                self._output_type = numpy_type_from_p4p_type[
                    response.type()["value"][-1]
                ]

            if is_enum:
                # We forward enum as string
                self._output_type = np.unicode_
                self._get_value = lambda resp: resp.value.choices[resp.value.index]
            else:
                self._get_value = lambda resp: resp.value
        except KeyError:
            self._logger.error(
                f"Don't know what numpy dtype to use for channel type {type(response)}"
            )

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
                    self._pv_name,
                    self._cached_update[1],
                    _get_alarm_status(self._cached_update[0]),
                    epics_alarm_severity_to_f142[self._cached_update[0].alarm.severity],
                )

    def stop(self):
        """
        Stop periodic updates and unsubscribe from PV
        """
        if self._repeating_timer is not None:
            self._repeating_timer.cancel()
        self._sub.close()
