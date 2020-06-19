from forwarder.epics_to_serialisable_types import numpy_type_from_channel_type
import numpy as np
from logging import Logger
from p4p.nt.enum import ntenum
from p4p import Value
from typing import Callable
from p4p.client.thread import Context


class PVAUpdateHandler:
    """
    Monitors via EPICS v4 Process Variable Access (PVA),
    serialises updates in FlatBuffers and passes them onto an Kafka Producer.
    PVA support from p4p library.
    """

    def unsubscribe(self):
        self._sub.close()

    def get_values(self, response: Value):
        value = np.array(self._get_value(response))
        status = response.raw.alarm.status
        severity = response.raw.alarm.severity
        return value, status, severity

    def subscribe(self, context: Context, pv_name: str, monitor_callback: Callable):
        self._sub = context.monitor(pv_name, monitor_callback)

    @staticmethod
    def get_timestamp(response: Value):
        return (
            response.raw.timeStamp.secondsPastEpoch * 1_000_000_000
        ) + response.raw.timeStamp.nanoseconds

    def get_epics_type(self, response: Value, logger: Logger):
        try:
            self._output_type = numpy_type_from_channel_type[type(response)]
            if type(response) is ntenum:
                self._get_value = lambda resp: resp.raw.value.index
            else:
                self._get_value = lambda resp: resp.raw.value
        except KeyError:
            logger.error(
                f"Don't know what numpy dtype to use for channel type "
                f"{type(response)}"
            )
            raise
