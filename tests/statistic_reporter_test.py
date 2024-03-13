import logging
from unittest.mock import ANY, MagicMock, call

import pytest

from forwarder.metrics import Counter, Gauge, Summary
from forwarder.metrics.statistics_reporter import StatisticsReporter

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


@pytest.mark.parametrize(
    "metric",
    [
        Counter("some_counter", "Description"),
        Gauge("some_gauge", "Description"),
        Summary("some_summary", "Description"),
    ],
)
def test_successful_metric_registration(metric):
    statistics_reporter = StatisticsReporter("localhost", logger)

    statistics_reporter.register_metric(metric.name, metric)

    assert len(statistics_reporter._metrics) == 1
    assert metric.name in statistics_reporter._metrics.keys()


def test_register_unsupported_metric_raises():
    statistics_reporter = StatisticsReporter("localhost", logger)
    from prometheus_client import Histogram

    metric_name = "some_histogram"
    metric = Histogram(metric_name, "Description")

    with pytest.raises(TypeError):
        statistics_reporter.register_metric(metric_name, metric)  # type: ignore
    assert len(statistics_reporter._metrics) == 0
    assert metric_name not in statistics_reporter._metrics.keys()


@pytest.mark.parametrize(
    "metric",
    [
        Counter("some_counter2", "Description"),
        Gauge("some_gauge2", "Description"),
        Summary("some_summary2", "Description"),
    ],
)
def test_successful_metric_deregistration(metric):
    statistics_reporter = StatisticsReporter("localhost", logger)

    statistics_reporter.register_metric(metric.name, metric)
    assert metric.name in statistics_reporter._metrics.keys()

    statistics_reporter.deregister_metric(metric.name)
    assert len(statistics_reporter._metrics) == 0
    assert metric.name not in statistics_reporter._metrics.keys()


def test_deregistration_of_unregistered_metric_is_ignored():
    statistics_reporter = StatisticsReporter("localhost", logger)
    registered_metric = Counter("registered_metric", "Description")
    statistics_reporter.register_metric(registered_metric.name, registered_metric)

    unregistered_metric = Counter("unregistered_metric", "Description")
    statistics_reporter.deregister_metric(unregistered_metric.name)
    assert len(statistics_reporter._metrics) == 1
    assert registered_metric.name in statistics_reporter._metrics.keys()
    assert unregistered_metric.name not in statistics_reporter._metrics.keys()


def test_that_warning_logged_on_send_exception(caplog):
    statistics_reporter = StatisticsReporter("localhost", logger)
    buffer_err_counter = Counter("some_buffer_errors_total", "Description")
    statistics_reporter.register_metric(buffer_err_counter.name, buffer_err_counter)
    statistics_reporter._sender = MagicMock()
    statistics_reporter._sender.send.side_effect = ValueError

    with caplog.at_level(logging.WARNING):
        statistics_reporter.send_statistics()

    assert caplog.text != ""


def test_statistic_reporter_performs_push():
    statistics_reporter = StatisticsReporter("localhost", logger)
    statistics_reporter._sender = MagicMock()
    counter = Counter("a_counter", "description")
    gauge = Gauge("a_gauge", "a description")
    statistics_reporter.register_metric(counter.name, counter)
    statistics_reporter.register_metric(gauge.name, gauge)

    counter.inc()
    counter.inc()
    gauge.set(123)
    statistics_reporter.send_statistics()

    calls = [
        call(counter.name, 2, ANY),
        call(gauge.name, 123, ANY),
    ]
    statistics_reporter._sender.send.assert_has_calls(calls, any_order=True)


def test_metrics_of_type_summary_produce_sum_and_count_metrics():
    statistics_reporter = StatisticsReporter("localhost", logger)
    statistics_reporter._sender = MagicMock()
    metric = Summary("a_summary", "description")
    statistics_reporter.register_metric(metric.name, metric)

    metric.observe(10)
    metric.observe(20)
    statistics_reporter.send_statistics()

    calls = [
        call(f"{metric.name}.sum", 30, ANY),
        call(f"{metric.name}.count", 2, ANY),
    ]
    statistics_reporter._sender.send.assert_has_calls(calls, any_order=True)
