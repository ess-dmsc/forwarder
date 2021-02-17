import logging
from threading import Thread
import time
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
    # Move this to utils
    from functools import wraps

    def run_in_thread(original):
        @wraps(original)
        def wrapper(*args, **kwargs):
            t = Thread(target=original, args=args, kwargs=kwargs, daemon=True)
            t.start()
            return t

        return wrapper

    class _DummyProducer:
        def __init__(self, counter, n_updates):
            self.counter = counter
            self.n_updates = n_updates

        @run_in_thread
        def produce(self):
            for _ in range(self.n_updates):
                self.counter.increment()

    timestamp = 100000
    mock_time.time.return_value = timestamp

    update_msg_counter: Counter = Counter()
    # This dictionary is of type Dict[Channel, UpdateHandler]
    # StatisticReporter only uses len of this dictionary
    update_handler = {"key1": "value1", "key2": "value2"}

    n_updates = 100
    dummy_producer = _DummyProducer(update_msg_counter, n_updates)

    statistics_reporter = StatisticsReporter("localhost", update_handler, update_msg_counter, logger, update_interval_s=0.5)  # type: ignore
    statistics_reporter._sender = MagicMock()

    fake_prod_t1 = dummy_producer.produce()
    fake_prod_t2 = dummy_producer.produce()
    statistics_reporter.start()

    fake_prod_t1.join()
    fake_prod_t2.join()
    # This wait time >= update_interval_s makes sure send_statistics
    # is called one more time after fake producers are done incrementing counter
    time.sleep(0.5)

    statistics_reporter.stop()
    # Test that _update_msg_counter records correct num updates
    assert statistics_reporter._update_msg_counter.value == 2 * n_updates
    # Test that calls to graphite server are made with correct arguments
    calls = [
        call("number_pvs", len(update_handler.keys()), timestamp),
        call("total_updates", 2 * n_updates, timestamp),
    ]
    statistics_reporter._sender.send.assert_has_calls(calls, any_order=True)
