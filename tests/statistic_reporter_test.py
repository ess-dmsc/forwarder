import time
import unittest
from unittest.mock import MagicMock

from forwarder.application_logger import setup_logger
from forwarder.statistics_reporter import StatisticsReporter


class TestStatisticReporter(unittest.TestCase):
    def setUp(self):
        logger = setup_logger()
        self.instance = StatisticsReporter("localhost", logger)
        self.instance._sender = MagicMock()

    def test_send_pv_numbers_exception(self):
        # Test that logger logs when
        # graphyte.Sender.send() method raises Exception
        self.instance._sender.send.side_effect = ValueError
        with self.assertLogs("python-forwarder") as cm:
            self.instance.send_pv_numbers("NOTINTORFLOAT", time.time())
        self.assertIn("ERROR:python-forwarder:Could not send statistic: ", cm.output)

    def test_send_pv_numbers(self):
        # test if first message is sent
        t1 = int(time.time())
        self.instance.send_pv_numbers(2, t1)
        self.assertEqual(t1, self.instance._last_update_s)

        # Wait for update_interval / 2 seconds
        # Test that message is not sent
        time.sleep(self.instance._update_interval_s / 2.0)
        t2 = int(time.time())
        self.instance.send_pv_numbers(2, t2)
        self.instance._sender.send.assert_called_once()
        self.assertNotEqual(t2, self.instance._last_update_s)

        # wait for another update_interval / 2 seconds + 2 seconds
        # test that message gets sent
        time.sleep(self.instance._update_interval_s / 2.0 + 2)
        t3 = int(time.time())
        self.instance.send_pv_numbers(2, t3)
        self.assertEqual(self.instance._sender.send.call_count, 2)
        self.assertEqual(t3, self.instance._last_update_s)
