import time
from random import randint
from typing import List

import numpy as np
from p4p.nt import NTScalar

from forwarder.application_logger import get_logger
from forwarder.repeat_timer import RepeatTimer, milliseconds_to_seconds
from forwarder.update_handlers.serialiser_tracker import SerialiserTracker


class FakeUpdateHandler:
    """
    Periodically generate a random integer as a PV value instead of monitoring a real EPICS PV
    serialises updates in FlatBuffers and passes them onto an Kafka Producer.
    """

    def __init__(
        self,
        serialiser_tracker_list: List[SerialiserTracker],
        schema: str,
        fake_pv_period_ms: int,
    ):
        self._logger = get_logger()
        self.serialiser_tracker_list: List[SerialiserTracker] = serialiser_tracker_list
        self._schema = schema

        self._repeating_timer = RepeatTimer(
            milliseconds_to_seconds(fake_pv_period_ms), self._timer_callback
        )
        self._repeating_timer.start()

    def _timer_callback(self):
        if self._schema == "tdct":
            # tdct needs a 1D array as data to send
            data = np.array([randint(0, 100)]).astype(np.int32)
            response = NTScalar("ai").wrap(data)
        else:
            # Otherwise 0D (scalar) is fine
            response = NTScalar("i").wrap(randint(0, 100))
        response.timeStamp["secondsPastEpoch"] = int(time.time())
        try:
            for serialiser_tracker in self.serialiser_tracker_list:
                serialiser_tracker.process_pva_message(response)
        except (RuntimeError, ValueError) as e:
            self._logger.error(
                f"Got error when handling PVA update. Message was: {str(e)}"
            )

    def stop(self):
        """
        Stop periodic updates
        """
        for serialiser in self.serialiser_tracker_list:
            serialiser.stop()
        self._repeating_timer.cancel()
