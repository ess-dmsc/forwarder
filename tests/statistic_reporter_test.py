import logging
from threading import Thread
from typing import Dict
from unittest.mock import MagicMock, patch, call

from forwarder.statistics_reporter import StatisticsReporter
from forwarder.utils import Counter


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def test_that_warning_logged_on_send_exception(caplog):
    update_handler: Dict = {}
    statistics_reporter = StatisticsReporter(
        "localhost", update_handler, Counter(), logger
    )
    statistics_reporter._sender = MagicMock()
    statistics_reporter._sender.send.side_effect = ValueError

    with caplog.at_level(logging.WARNING):
        statistics_reporter.send_statistics()
    assert "Could not send statistic: " in caplog.text


@patch("forwarder.statistics_reporter.time")
def test_that_send_statistics_sends_correct_messages(mock_time):
    def worker(counter):
        for i in range(1000):
            counter.increment()

    timestamp = 100000
    mock_time.time.return_value = timestamp

    update_msg_counter: Counter = Counter()

    update_handler: Dict = {}
    statistics_reporter = StatisticsReporter(
        "localhost", update_handler, update_msg_counter, logger
    )
    statistics_reporter._sender = MagicMock()

    t1 = Thread(target=worker, args=(update_msg_counter,))
    t2 = Thread(target=statistics_reporter.send_statistics)

    t1.start()
    t2.start()
    t1.join()
    t2.join()
    # Test that _update_msg_counter records correct num updates
    assert statistics_reporter._update_msg_counter.value == 1000
    # Test that calls to graphite server are made with correct arguments
    calls = [
        call("number_pvs", len(update_handler.keys()), timestamp),
        # TODO: Test for calls("total_updates", ...)
    ]
    statistics_reporter._sender.send.assert_has_calls(calls, any_order=True)
