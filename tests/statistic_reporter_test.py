import logging
import time
from unittest.mock import MagicMock

from forwarder.statistics_reporter import StatisticsReporter


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def test_that_warning_logged_on_send_exception(caplog):
    statistics_reporter = StatisticsReporter("localhost", logger)
    statistics_reporter._sender = MagicMock()
    statistics_reporter._sender.send.side_effect = ValueError
    with caplog.at_level(logging.WARNING):
        statistics_reporter.send_pv_numbers("NOTINTORFLOAT", time.time())
    assert "Could not send statistic: " in caplog.text


def test_that_send_called_only_after_update_intervals():
    statistics_reporter = StatisticsReporter("localhost", logger)
    statistics_reporter._sender = MagicMock()

    # test that first message is sent
    first_msg_time = int(time.time())
    statistics_reporter.send_pv_numbers(2, first_msg_time)
    assert first_msg_time == statistics_reporter._last_update_s

    # Test that message is not sent before update_interval time is complete
    second_msg_time = first_msg_time + statistics_reporter._update_interval_s // 2
    statistics_reporter.send_pv_numbers(2, second_msg_time)
    statistics_reporter._sender.send.assert_called_once()
    assert second_msg_time != statistics_reporter._last_update_s

    # Test that message gets sent after update_interval time is complete
    third_msg_time = second_msg_time + statistics_reporter._update_interval_s // 2 + 2
    statistics_reporter.send_pv_numbers(2, third_msg_time)
    assert statistics_reporter._sender.send.call_count == 2
    assert third_msg_time == statistics_reporter._last_update_s
