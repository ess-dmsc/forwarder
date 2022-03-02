from typing import Optional

from caproto import ReadNotifyResponse
from caproto.threading.client import PV
from caproto.threading.client import Context as CAContext

from forwarder.kafka.kafka_producer import KafkaProducer
from forwarder.update_handlers.base_update_handler import (
    BaseUpdateHandler,
)


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
        super().__init__(producer, pv_name, output_topic, schema, periodic_update_ms)

        (self._pv,) = context.get_pvs(
            pv_name, connection_state_callback=self._connection_state_callback
        )
        # Subscribe with "data_type='time'" to get timestamp and alarm fields
        sub = self._pv.subscribe(data_type="time")
        sub.add_callback(self._monitor_callback)

    def _monitor_callback(self, sub, response: ReadNotifyResponse):
        try:
            for serialiser_tracker in self.serialiser_tracker_list:
                new_message, new_timestamp = serialiser_tracker.serialiser.ca_serialise(
                    response
                )
                if new_message is not None:
                    serialiser_tracker.set_new_message(new_message, new_timestamp)
        except (RuntimeError, ValueError) as e:
            self._logger.error(
                f"Got error when handling CA update. Message was: {str(e)}"
            )

    def _connection_state_callback(self, pv: PV, state: str):
        try:
            for serialiser_tracker in self.serialiser_tracker_list:
                (
                    new_message,
                    new_timestamp,
                ) = serialiser_tracker.serialiser.ca_conn_serialise(pv, state)
                if new_message is not None:
                    serialiser_tracker.set_new_message(new_message, new_timestamp)
        except (RuntimeError, ValueError) as e:
            self._logger.error(
                f"Got error when handling CA connection status. Message was: {str(e)}"
            )

    def stop(self):
        """
        Stop periodic updates and unsubscribe from PV
        """
        super().stop()
        self._pv.unsubscribe_all()
