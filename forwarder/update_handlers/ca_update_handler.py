import time
from typing import List, Optional

from caproto import ReadNotifyResponse
from caproto.threading.client import PV
from caproto.threading.client import Context as CAContext

from forwarder.application_logger import get_logger
from forwarder.metrics import Counter, Summary, sanitise_metric_name
from forwarder.metrics.statistics_reporter import StatisticsReporter
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
        statistics_reporter: Optional[StatisticsReporter] = None,
        processing_errors_metric: Optional[Counter] = None,
    ):
        self._logger = get_logger()
        self.serialiser_tracker_list: List[SerialiserTracker] = serialiser_tracker_list
        self._current_unit = None
        self._pv_name = pv_name
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
                self._logger.warning(f"Could not initialise metric: {e}")

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
            if self._processing_errors_metric:
                self._processing_errors_metric.inc()

    def _monitor_callback(self, sub, response: ReadNotifyResponse):
        if self._receive_latency_metric:
            try:
                response_timestamp = response.metadata.timestamp.seconds + (
                    response.metadata.timestamp.nanoseconds / 1_000_000_000
                )
                self._receive_latency_metric.observe(time.time() - response_timestamp)
            except Exception as e:
                self._logger.warning(f"Could not calculate receive latency: {str(e)}")
        try:
            if self._processing_latency_metric:
                with self._processing_latency_metric.time():
                    for serialiser_tracker in self.serialiser_tracker_list:
                        serialiser_tracker.process_ca_message(response)
            else:
                for serialiser_tracker in self.serialiser_tracker_list:
                    serialiser_tracker.process_ca_message(response)
        except (RuntimeError, ValueError) as e:
            self._logger.error(
                f"Got error when handling CA update. Message was: {str(e)}"
            )
            if self._processing_errors_metric:
                self._processing_errors_metric.inc()
        except BaseException as e:
            exception_string = f"Got uncaught exception in CAUpdateHandler._monitor_callback. The message was: {str(e)}"
            self._logger.error(exception_string)
            self._logger.exception(e)
            if self._processing_errors_metric:
                self._processing_errors_metric.inc()

    def _connection_state_callback(self, pv: PV, state: str):
        try:
            for serialiser_tracker in self.serialiser_tracker_list:
                serialiser_tracker.process_ca_connection(pv, state)
        except (RuntimeError, ValueError) as e:
            self._logger.error(
                f"Got error when handling CA connection status. Message was: {str(e)}"
            )
            if self._processing_errors_metric:
                self._processing_errors_metric.inc()
        except BaseException as e:
            exception_string = f"Got uncaught exception in CAUpdateHandler._connection_state_callback. The message was: {str(e)}"
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
        self._pv.unsubscribe_all()
