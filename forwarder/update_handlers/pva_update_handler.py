import time
from typing import List, Optional, Union

from p4p.client.thread import Context as PVAContext
from p4p.client.thread import Value

from forwarder.application_logger import get_logger
from forwarder.metrics import Counter, Summary, sanitise_metric_name
from forwarder.metrics.statistics_reporter import StatisticsReporter
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
        statistics_reporter: Optional[StatisticsReporter] = None,
        processing_errors_metric: Optional[Counter] = None,
    ):
        self._logger = get_logger()
        self.serialiser_tracker_list: List[SerialiserTracker] = serialiser_tracker_list
        self._pv_name = pv_name
        self._unit = None
        self._statistics_reporter = statistics_reporter
        self._processing_errors_metric = processing_errors_metric
        self._processing_latency_metric = None
        self._receive_latency_metric = None
        if self._statistics_reporter:
            try:
                self._processing_latency_metric = Summary(
                    sanitise_metric_name(f"processing_latency_seconds.{self._pv_name}"),
                    "Time from the reception of the EPICS update until the Kafka produce call returns",
                )
                self._statistics_reporter.register_metric(
                    f"processing_latency_seconds.{self._pv_name}",
                    self._processing_latency_metric,
                )
                self._receive_latency_metric = Summary(
                    sanitise_metric_name(f"receive_latency_seconds.{self._pv_name}"),
                    "Time difference between the EPICS timestamp and the reception time at the Forwarder",
                )
                self._statistics_reporter.register_metric(
                    f"receive_latency_seconds.{self._pv_name}",
                    self._receive_latency_metric,
                )
            except Exception as e:
                self._logger.warning(f"Could not initialise metric for {pv_name}: {e}")

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
            if self._processing_errors_metric:
                self._processing_errors_metric.inc()
        if self._receive_latency_metric and isinstance(response, Value):
            try:
                response_timestamp = response.timeStamp.secondsPastEpoch + (
                    response.timeStamp.nanoseconds / 1_000_000_000
                )
                self._receive_latency_metric.observe(time.time() - response_timestamp)
            except Exception as e:
                self._logger.warning(f"Could not calculate receive latency: {str(e)}")
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
        except BaseException as e:
            exception_string = f"Got uncaught exception in PVAUpdateHandler._monitor_callback. The message was: {str(e)}"
            self._logger.error(exception_string)
            self._logger.exception(e)
            if self._processing_errors_metric:
                self._processing_errors_metric.inc()

    def stop(self):
        """
        Stop periodic updates and unsubscribe from PV
        """
        for serialiser in self.serialiser_tracker_list:
            serialiser.stop()
        if self._statistics_reporter:
            self._statistics_reporter.deregister_metric(
                f"processing_latency_seconds.{self._pv_name}"
            )
            self._statistics_reporter.deregister_metric(
                f"receive_latency_seconds.{self._pv_name}"
            )
        self._sub.close()
