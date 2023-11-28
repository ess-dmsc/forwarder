import time
from random import randint
from typing import List, Optional

import numpy as np
from numpy.typing import NDArray
from p4p.nt import NTScalar

from forwarder.application_logger import get_logger
from forwarder.metrics import Counter
from forwarder.metrics.statistics_reporter import StatisticsReporter
from forwarder.repeat_timer import RepeatTimer, milliseconds_to_seconds
from forwarder.update_handlers.serialiser_tracker import SerialiserTracker


class FakeUpdateHandler:
    """
    Periodically generate a random integer as a PV value instead of monitoring a real EPICS PV
    serialises updates in FlatBuffers and passes them onto a Kafka Producer.
    """

    def __init__(
        self,
        serialiser_tracker_list: List[SerialiserTracker],
        schema: str,
        fake_pv_period_ms: int,
        statistics_reporter: Optional[StatisticsReporter] = None,
        processing_errors_metric: Optional[Counter] = None,
    ):
        self._logger = get_logger()
        self.serialiser_tracker_list: List[SerialiserTracker] = serialiser_tracker_list
        self._schema = schema
        self._statistics_reporter = statistics_reporter
        self._processing_errors_metric = processing_errors_metric
        self._processing_latency_metric = None
        self._receive_latency_metric = None

        self._repeating_timer = RepeatTimer(
            milliseconds_to_seconds(fake_pv_period_ms), self._timer_callback
        )
        self._repeating_timer.start()

    def _timer_callback(self):
        if self._schema == "tdct":
            # tdct needs a 1D array as data to send
            data: NDArray[np.int32] = np.array([randint(0, 100)]).astype(np.int32)
            response = NTScalar("ai").wrap(data)
        else:
            # Otherwise 0D (scalar) is fine
            response = NTScalar("i").wrap(randint(0, 100))
        response.timeStamp["secondsPastEpoch"] = int(time.time())
        try:
            if self._processing_latency_metric:
                with self._processing_latency_metric.time():
                    for serialiser_tracker in self.serialiser_tracker_list:
                        serialiser_tracker.process_pva_message(response)
            else:
                for serialiser_tracker in self.serialiser_tracker_list:
                    serialiser_tracker.process_pva_message(response)
        except (RuntimeError, ValueError) as e:
            self._logger.error(
                f"Got error when handling PVA update. Message was: {str(e)}"
            )
            if self._processing_errors_metric:
                self._processing_errors_metric.inc()

    def stop(self):
        """
        Stop periodic updates
        """
        for serialiser in self.serialiser_tracker_list:
            serialiser.stop()
        self._repeating_timer.cancel()
