import time
from threading import Lock
from typing import Optional, Union

from p4p import Value
from p4p.client.thread import Cancelled
from p4p.client.thread import Context as PVAContext
from p4p.client.thread import Disconnected, RemoteError

from forwarder.kafka.kafka_helpers import (
    publish_connection_status_message,
    seconds_to_nanoseconds,
)
from forwarder.kafka.kafka_producer import KafkaProducer
from forwarder.update_handlers.base_update_handler import BaseUpdateHandler, SerialiserTracker
from forwarder.update_handlers.schema_serialisers import schema_serialisers


class PVAUpdateHandler(BaseUpdateHandler):
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
        super().__init__(producer, pv_name, output_topic, schema, periodic_update_ms)

        request = context.makeRequest("field()")
        self._sub = context.monitor(
            self._pv_name,
            self._monitor_callback,
            request=request,
            notify_disconnect=True,
        )

    def _monitor_callback(self, response: Union[Value, Exception]):
        try:
            for serialiser_tracker in self.serialiser_tracker_list:
                new_message, new_timestamp = serialiser_tracker.serialiser.pva_serialise(response)
                if new_message is not None:
                    serialiser_tracker.set_new_message(new_message, new_timestamp)
        except (RuntimeError, ValueError) as e:
            self._logger.error(
                f"Got error when handling PVA update. Message was: {str(e)}"
            )


    def stop(self):
        """
        Stop periodic updates and unsubscribe from PV
        """
        super().stop()
        self._sub.close()
