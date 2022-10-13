from typing import List

from caproto import ReadNotifyResponse
from caproto.threading.client import PV
from caproto.threading.client import Context as CAContext

from forwarder.application_logger import get_logger
from forwarder.update_handlers.serialiser_tracker import SerialiserTracker


class CAUpdateHandler:
    """
    Monitors via EPICS v3 Channel Access (CA),
    serialises updates in FlatBuffers and passes them onto an Kafka Producer.
    CA support from caproto library.
    """

    def __init__(
        self,
        context: CAContext,
        pv_name: str,
        serialiser_tracker_list: List[SerialiserTracker],
    ):
        self._logger = get_logger()
        self.serialiser_tracker_list: List[SerialiserTracker] = serialiser_tracker_list
        self._current_unit = None
        self._pv_name = pv_name

        (self._pv,) = context.get_pvs(
            pv_name, connection_state_callback=self._connection_state_callback
        )
        # Subscribe with "data_type='time'" to get timestamp and alarm fields
        sub = self._pv.subscribe(data_type="time")
        sub.add_callback(self._monitor_callback)

        ctrl_sub = self._pv.subscribe(data_type="control")
        ctrl_sub.add_callback(self._unit_callback)

    def _unit_callback(self, sub, response: ReadNotifyResponse):
        old_unit = self._current_unit
        try:
            self._current_unit = response.metadata.units.decode("utf-8")
        except AttributeError:
            return
        if old_unit is not None and old_unit != self._current_unit:
            self._logger.error(
                f'Display unit of (ca) PV with name "{self._pv_name}" changed from "{old_unit}" to "{self._current_unit}".'
            )

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
        except BaseException as e:
            exception_string = f"Got uncaught exception in CAUpdateHandler._monitor_callback. The message was: {str(e)}"
            self._logger.error(exception_string)
            self._logger.exception(e)

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
        except BaseException as e:
            exception_string = f"Got uncaught exception in CAUpdateHandler._connection_state_callback. The message was: {str(e)}"
            self._logger.error(exception_string)
            self._logger.exception(e)

    def stop(self):
        """
        Stop periodic updates and unsubscribe from PV
        """
        for serialiser in self.serialiser_tracker_list:
            serialiser.stop()
        self._pv.unsubscribe_all()
