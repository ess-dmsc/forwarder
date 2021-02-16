import logging
import queue
from threading import Thread
from typing import Dict
from unittest.mock import MagicMock, patch, call

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
def test_that_send_statistics_sends_correct_messages(mock_time):
    def worker(q):
        msgs = [
            "pv_name-pva",
            "pv_name-pva",
            "pv_name-ca",
            "pv_name-pva",
            "pv_name-ca",
            "pv_name-fake",
        ]
        for msg in msgs:
            q.put(msg)

    timestamp = 100000
    mock_time.time.return_value = timestamp

    update_msg_queue: queue.Queue = queue.Queue()

    update_handler: Dict = {}
    statistics_reporter = StatisticsReporter(
        "localhost", update_handler, update_msg_queue, logger
    )
    statistics_reporter._sender = MagicMock()

    t1 = Thread(target=worker, args=(update_msg_queue,))
    t2 = Thread(target=statistics_reporter.send_statistics)

    t1.start()
    t2.start()
    t1.join()
    t2.join()
    # Test that channel _updates_counter records correct num updates
    assert statistics_reporter._updates_counter == {
        "pv_name-pva": 3,
        "pv_name-ca": 2,
        "pv_name-fake": 1,
    }
    # Test that calls to graphite server are made with correct arguments
    calls = [
        call("number_pvs", len(update_handler.keys()), timestamp),
        call("pv_name-pva", 3, timestamp),
        call("pv_name-ca", 2, timestamp),
        call("pv_name-fake", 1, timestamp),
        call("total_updates", 6, timestamp),
    ]
    statistics_reporter._sender.send.assert_has_calls(calls, any_order=True)
