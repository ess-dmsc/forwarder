import time
from logging import Logger
from typing import Dict, Union, get_args

import graphyte  # type: ignore

from forwarder.metrics import Counter, Gauge, Summary
from forwarder.repeat_timer import RepeatTimer

MetricType = Union[Counter, Summary, Gauge]


class StatisticsReporter:
    def __init__(
        self,
        graphyte_server: str,
        logger: Logger,
        prefix: str = "forwarder",
        update_interval_s: int = 10,
    ):
        self._graphyte_server = graphyte_server
        self._logger = logger
        self._sender = graphyte.Sender(self._graphyte_server, prefix=prefix)
        self._repeating_timer = RepeatTimer(update_interval_s, self.send_statistics)
        self._metrics: Dict[str, MetricType] = {}

    def register_metric(self, name: str, metric: MetricType):
        if not isinstance(metric, get_args(MetricType)):
            raise TypeError(f"Unsupported metric type: {type(metric).__name__}")
        self._metrics[name] = metric

    def deregister_metric(self, name: str):
        if name in self._metrics:
            del self._metrics[name]

    def start(self):
        self._repeating_timer.start()

    def send_statistics(self):
        try:
            for metric_name, metric in self._metrics.items():
                timestamp = time.time()
                if isinstance(metric, (Counter, Gauge)):
                    self._sender.send(metric_name, metric.value, timestamp)
                elif isinstance(metric, Summary):
                    self._sender.send(f"{metric_name}.sum", metric.sum, timestamp)
                    self._sender.send(f"{metric_name}.count", metric.count, timestamp)
                    self._sender.send(
                        f"{metric_name}.average",
                        metric.sum / metric.count if metric.count > 0 else 0,
                        timestamp,
                    )
        except Exception as ex:
            self._logger.error(f"Could not send statistics: {ex}")

    def stop(self):
        if self._repeating_timer:
            self._repeating_timer.cancel()
