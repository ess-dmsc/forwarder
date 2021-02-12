import logging
import queue
from typing import Dict
from unittest.mock import MagicMock, patch

from forwarder.statistics_reporter import StatisticsReporter


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def test_that_warning_logged_on_send_exception(caplog):
    update_handler: Dict = {}
    statistics_reporter = StatisticsReporter(
        "localhost", update_handler, queue.Queue(), logger
    )
    statistics_reporter._sender = MagicMock()
    statistics_reporter._sender.send.side_effect = ValueError

    with caplog.at_level(logging.WARNING):
        statistics_reporter.send_statistics()
    assert "Could not send statistic: " in caplog.text


@patch("forwarder.statistics_reporter.time")
def test_that_send_statistics_sends_correct_message(mock_time):
    timestamp = 100000
    mock_time.time.return_value = timestamp

    update_handler: Dict = {}
    statistics_reporter = StatisticsReporter(
        "localhost", update_handler, queue.Queue(), logger
    )
    statistics_reporter._sender = MagicMock()

    statistics_reporter.send_statistics()
    statistics_reporter._sender.send.assert_called_once_with(
        "number_pvs", len(update_handler.keys()), timestamp
    )
