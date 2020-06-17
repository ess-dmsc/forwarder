from forwarder.epics_to_serialisable_types import numpy_type_from_channel_type
import numpy as np
from p4p.nt.enum import ntenum
from forwarder.update_handlers.base_update_handler import BaseUpdateHandler


class PVAUpdateHandler(BaseUpdateHandler):
    """
    Monitors via EPICS v4 Process Variable Access (PVA),
    serialises updates in FlatBuffers and passes them onto an Kafka Producer.
    PVA support from p4p library.
    """

    def _unsubscribe(self):
        self._sub.close()

    def _get_values(self, response):
        value = np.array(self._get_value(response))
        status = response.raw.alarm.status
        severity = response.raw.alarm.severity
        return value, status, severity

    def _subscribe(self, context, pv_name):
        self._sub = context.monitor(pv_name, self._monitor_callback)

    def _get_timestamp(self, response):
        return (
            response.raw.timeStamp.secondsPastEpoch * 1_000_000_000
        ) + response.raw.timeStamp.nanoseconds

    def _get_epics_type(self, response):
        try:
            self._output_type = numpy_type_from_channel_type[type(response)]
            if type(response) is ntenum:
                self._get_value = lambda resp: resp.raw.value.index
            else:
                self._get_value = lambda resp: resp.raw.value
        except KeyError:
            self._logger.error(
                f"Don't know what numpy dtype to use for channel type "
                f"{type(response)}"
            )
            raise
