from typing import List, Union

from p4p.client.thread import Context as PVAContext
from p4p.client.thread import Value

from forwarder.application_logger import get_logger
from forwarder.update_handlers.serialiser_tracker import SerialiserTracker


class PVAUpdateHandler:
    """
    Monitors via EPICS v4 Process Variable Access (PVA),
    serialises updates in FlatBuffers and passes them onto a Kafka Producer.
    PVA support from p4p library.
    """

    def __init__(
        self,
        context: PVAContext,
        pv_name: str,
        serialiser_tracker_list: List[SerialiserTracker],
    ):
        self._logger = get_logger()
        self.serialiser_tracker_list: List[SerialiserTracker] = serialiser_tracker_list
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
            self._unit = response.display.units  # type: ignore
        except AttributeError:
            pass
        if old_unit is not None and old_unit != self._unit:
            self._logger.error(
                f'Display unit of (pva) PV with name "{self._pv_name}" changed from "{old_unit}" to "{self._unit}".'
            )
        try:
            for serialiser_tracker in self.serialiser_tracker_list:
                serialiser_tracker.process_pva_message(response)
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
        for serialiser in self.serialiser_tracker_list:
            serialiser.stop()
        self._sub.close()
