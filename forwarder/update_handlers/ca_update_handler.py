import time
from threading import Lock
from typing import Optional

from caproto import ReadNotifyResponse
from caproto.threading.client import PV
from caproto.threading.client import Context as CAContext

from forwarder.kafka.kafka_helpers import (
    publish_connection_status_message,
    seconds_to_nanoseconds,
)
from forwarder.kafka.kafka_producer import KafkaProducer
from forwarder.repeat_timer import RepeatTimer, milliseconds_to_seconds
from forwarder.update_handlers.schema_serialisers import schema_serialisers
from forwarder.update_handlers.base_update_handler import BaseUpdateHandler


class CAUpdateHandler(BaseUpdateHandler):
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
        super().__init__(producer, pv_name, output_topic)
        self._cached_update: Optional[ReadNotifyResponse] = None
        self._repeating_timer = None
        self._cache_lock = Lock()

        if schema not in schema_serialisers:
            raise ValueError(
                f"{schema} is not a recognised supported schema, use one of {list(schema_serialisers.keys())}"
            )
        self._message_serialiser = schema_serialisers[schema](pv_name)

        (self._pv,) = context.get_pvs(
            pv_name, connection_state_callback=self._connection_state_callback
        )
        # Subscribe with "data_type='time'" to get timestamp and alarm fields
        sub = self._pv.subscribe(data_type="time")
        sub.add_callback(self._monitor_callback)

        if periodic_update_ms is not None:
            self._repeating_timer = RepeatTimer(
                milliseconds_to_seconds(periodic_update_ms), self.publish_cached_update
            )
            self._repeating_timer.start()

    def _monitor_callback(self, sub, response: ReadNotifyResponse):
        # Skip PV updates with empty values
        try:
            if response.data.size == 0:
                return
        except AttributeError:
            # Enum values for example don't have .size, just continue
            pass

        with self._cache_lock:
            # If this is the first update or the alarm status has changed, then
            # include alarm status in message
            if (
                self._cached_update is None
                or response.metadata.status != self._cached_update.metadata.status
            ):
                self._publish_message(
                    *self._message_serialiser.serialise(response, serialise_alarm=True)
                )
            else:
                self._publish_message(
                    *self._message_serialiser.serialise(response, serialise_alarm=False)
                )
            self._cached_update = response
            if self._repeating_timer is not None:
                self._repeating_timer.reset()

    def _connection_state_callback(self, pv: PV, state: str):
        publish_connection_status_message(
            self._producer,
            self._output_topic,
            self._pv.name,
            seconds_to_nanoseconds(time.time()),
            state,
        )

    def publish_cached_update(self):
        with self._cache_lock:
            if self._cached_update is not None:
                # Always include current alarm status in periodic update messages
                self._publish_message(
                    *self._message_serialiser.serialise(
                        self._cached_update, serialise_alarm=True
                    )
                )

    def stop(self):
        """
        Stop periodic updates and unsubscribe from PV
        """
        if self._repeating_timer is not None:
            self._repeating_timer.cancel()
        self._pv.unsubscribe_all()
