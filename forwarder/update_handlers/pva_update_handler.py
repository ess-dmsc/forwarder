from typing import Union, List

from p4p import Value
from p4p.client.thread import Context as PVAContext
from forwarder.update_handlers.base_update_handler import (
    BaseUpdateHandler,
    SerialiserTracker,
)


class PVAUpdateHandler(BaseUpdateHandler):
    """
    Monitors via EPICS v4 Process Variable Access (PVA),
    serialises updates in FlatBuffers and passes them onto an Kafka Producer.
    PVA support from p4p library.
    """

    def __init__(
        self,
        context: PVAContext,
        pv_name: str,
        serialiser_tracker_list: List[SerialiserTracker],
    ):
        super().__init__(serialiser_tracker_list)
        self._pv_name = pv_name
        self._unit = None

        request = context.makeRequest("field()")
        self._sub = context.monitor(
            pv_name,
            self._monitor_callback,
            request=request,
            notify_disconnect=True,
        )

    def _monitor_callback(self, response: Union[Value, Exception]):
        old_unit = self._unit
        try:
            self._unit = response.display.units
        except AttributeError:
            pass
        if old_unit is not None and old_unit != self._unit:
            self._logger.error(
                f"Display unit of (pva) PV with name \"{self._pv_name}\" changed from \"{old_unit}\" to \"{self._unit}\"."
            )
        try:
            for serialiser_tracker in self.serialiser_tracker_list:
                (
                    new_message,
                    new_timestamp,
                ) = serialiser_tracker.serialiser.pva_serialise(response)
                if new_message is not None:
                    serialiser_tracker.set_new_message(new_message, new_timestamp)
        except (RuntimeError, ValueError) as e:
            self._logger.error(
                f"Got error when handling PVA update. Message was: {str(e)}"
            )
        except BaseException as e:
            exception_string = f"Got uncaught exception in PVAUpdateHandler._monitor_callback. The message was: {str(e)}"
            self._logger.error(exception_string)
            self._logger.exception(e)

    def stop(self):
        """
        Stop periodic updates and unsubscribe from PV
        """
        super().stop()
        self._sub.close()
