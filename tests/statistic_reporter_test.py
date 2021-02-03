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

    # test if first message is sent
    t1 = int(time.time())
    statistics_reporter.send_pv_numbers(2, t1)
    assert t1 == statistics_reporter._last_update_s

    # Wait for update_interval / 2 seconds
    # Test that message is not sent
    t2 = t1 + statistics_reporter._update_interval_s // 2
    statistics_reporter.send_pv_numbers(2, t2)
    statistics_reporter._sender.send.assert_called_once()
    assert t2 != statistics_reporter._last_update_s

    # wait for another update_interval / 2 seconds + 2 seconds
    # test that message gets sent
    t3 = t2 + statistics_reporter._update_interval_s // 2 + 2
    statistics_reporter.send_pv_numbers(2, t3)
    assert statistics_reporter._sender.send.call_count == 2
    assert t3 == statistics_reporter._last_update_s
