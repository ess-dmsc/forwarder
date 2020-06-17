from caproto import ChannelType
from forwarder.epics_to_serialisable_types import numpy_type_from_channel_type
from forwarder.update_handlers.base_update_handler import BaseUpdateHandler


def _seconds_to_nanoseconds(time_seconds: float) -> int:
    return int(time_seconds * 1_000_000_000)


class CAUpdateHandler(BaseUpdateHandler):
    """
    Monitors via EPICS v3 Channel Access (CA),
    serialises updates in FlatBuffers and passes them onto an Kafka
    Producer.
    CA support from caproto library.
    """

    def _unsubscribe(self):
        self._pv.unsubscribe_all()

    def _get_values(self, response):
        value = response.data
        status = response.metadata.status
        severity = response.metadata.severity
        return value, status, severity

    def __callback_wrapper(self, ignored, response):
        self._monitor_callback(response)

    def _subscribe(self, context, pv_name):
        (self._pv,) = context.get_pvs(pv_name)
        # Prevent our monitor timing out if PV is not available right now
        # This causes subscribe() to block so commenting out for now,
        # see ticket #10.
        # self._pv.timeout = None
        # Subscribe with "data_type='time'" to get timestamp and alarm fields
        sub = self._pv.subscribe(data_type="time")
        sub.add_callback(self.__callback_wrapper)

    def _get_timestamp(self, response):
        return _seconds_to_nanoseconds(response.metadata.timestamp)

    def _get_epics_type(self, response):
        try:
            return numpy_type_from_channel_type[response.data_type]
        except KeyError:
            self._logger.error(
                f"Don't know what numpy dtype to use for channel type "
                f"{ChannelType(response.data_type)}"
            )
            raise
