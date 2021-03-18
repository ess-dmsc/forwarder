import time
from threading import Lock
from typing import Any, Optional, Tuple, Union

import numpy as np
from p4p import Value
from p4p.client.thread import Cancelled
from p4p.client.thread import Context as PVAContext
from p4p.client.thread import Disconnected, RemoteError
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus

from forwarder.application_logger import get_logger
from forwarder.epics_to_serialisable_types import (
    epics_alarm_severity_to_f142,
    numpy_type_from_p4p_type,
    pva_alarm_message_to_f142_alarm_status,
)
from forwarder.kafka.kafka_helpers import (
    publish_connection_status_message,
    seconds_to_nanoseconds,
    _nanoseconds_to_milliseconds
)
from forwarder.kafka.kafka_producer import KafkaProducer
from forwarder.repeat_timer import RepeatTimer, milliseconds_to_seconds
from forwarder.update_handlers.schema_serialisers import schema_serialisers


# def _get_alarm_status(response):
#     try:
#         alarm_status = pva_alarm_message_to_f142_alarm_status[response.alarm.message]
#     except Exception:
#         alarm_status = AlarmStatus.UDF
#     return alarm_status


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
        self._pv_name = pv_name
        self._cached_update: Optional[Tuple[Value, int]] = None
        # self._output_type: Any = None
        self._repeating_timer = None
        self._cache_lock = Lock()

        try:
            self._message_serialiser = schema_serialisers[schema](self._pv_name)
        except KeyError:
            raise ValueError(
                f"{schema} is not a recognised supported schema, use one of {list(schema_serialisers.keys())}"
            )

        request = context.makeRequest("field()")
        self._sub = context.monitor(
            self._pv_name,
            self._monitor_callback,
            request=request,
            notify_disconnect=True,
        )

        if periodic_update_ms is not None:
            self._repeating_timer = RepeatTimer(
                milliseconds_to_seconds(periodic_update_ms), self.publish_cached_update
            )
            self._repeating_timer.start()

    def _monitor_callback(self, response: Union[Value, Exception]):
        if isinstance(response, Exception):
            # "Cancelled" occurs when we unsubscribe, we don't want to publish that as a
            # connection state change.
            # We are only interested loss of communication with the server
            if not isinstance(response, Cancelled):
                if isinstance(response, (Disconnected, RemoteError)):
                    connection_state = "disconnected"
                else:
                    connection_state = "unrecognised_connection_exception"
                publish_connection_status_message(
                    self._producer,
                    self._output_topic,
                    self._pv_name,
                    seconds_to_nanoseconds(time.time()),
                    connection_state,
                )
            return

        # Skip PV updates with empty values
        try:
            if response.value.size == 0:
                return
        except AttributeError:
            # Enum values for example don't have .size, just continue
            pass

        timestamp = (
            response.timeStamp.secondsPastEpoch * 1_000_000_000
        ) + response.timeStamp.nanoseconds
        # if self._output_type is None:
        #     self._try_to_determine_type(response)

        with self._cache_lock:
            # If this is the first update or the alarm status has changed, then
            # include alarm status in message
            if (
                self._cached_update is None
                or response.alarm.message != self._cached_update[0].alarm.message
            ):
                self._publish_message(self._message_serialiser.serialise(response, serialise_alarm=True), timestamp)
                # self._message_publisher(
                #     self._producer,
                #     self._output_topic,
                #     np.squeeze(np.array(self._get_value(response))).astype(
                #         self._output_type
                #     ),
                #     self._pv_name,
                #     timestamp,
                #     _get_alarm_status(response),
                #     epics_alarm_severity_to_f142[response.alarm.severity],
                # )
            else:
                self._publish_message(self._message_serialiser.serialise(response, serialise_alarm=False), timestamp)
                # self._message_publisher(
                #     self._producer,
                #     self._output_topic,
                #     np.squeeze(np.array(self._get_value(response))).astype(
                #         self._output_type
                #     ),
                #     self._pv_name,
                #     timestamp,
                # )
            self._cached_update = (response, timestamp)
            if self._repeating_timer is not None:
                self._repeating_timer.reset()

    # def _try_to_determine_type(self, response):
    #     try:
    #         is_enum = False
    #         try:
    #             if response.type()["value"].getID() == "enum_t":
    #                 is_enum = True
    #         except AttributeError:
    #             # Attribute error raised because getID doesn't exist for scalar and
    #             # scalar-array types.
    #             # Array output types are prefixed by "a", we don't need this.
    #             self._output_type = numpy_type_from_p4p_type[
    #                 response.type()["value"][-1]
    #             ]
    #
    #         if is_enum:
    #             # We forward enum as string
    #             self._output_type = np.unicode_
    #             self._get_value = lambda resp: resp.value.choices[resp.value.index]
    #         else:
    #             self._get_value = lambda resp: resp.value
    #     except KeyError:
    #         self._logger.error(
    #             f"Don't know what numpy dtype to use for channel type {type(response)}"
    #         )

    def publish_cached_update(self):
        with self._cache_lock:
            if self._cached_update is not None:
                # Always include current alarm status in periodic update messages
                self._publish_message(self._message_serialiser.serialise(self._cached_update[0], serialise_alarm=True), self._cached_update[1])
                # self._publish_message()
                # self._message_publisher(
                #     self._producer,
                #     self._output_topic,
                #     np.squeeze(
                #         np.array(self._get_value(self._cached_update[0]))
                #     ).astype(self._output_type),
                #     self._pv_name,
                #     self._cached_update[1],
                #     _get_alarm_status(self._cached_update[0]),
                #     epics_alarm_severity_to_f142[self._cached_update[0].alarm.severity],
                # )

    def _publish_message(self, message: bytes, timestamp_ns: int):
        self._producer.produce(self._output_topic, message, _nanoseconds_to_milliseconds(timestamp_ns))

    def stop(self):
        """
        Stop periodic updates and unsubscribe from PV
        """
        if self._repeating_timer is not None:
            self._repeating_timer.cancel()
        self._sub.close()
