from forwarder.update_handlers.base_update_handler import BaseUpdateHandler
from datetime import datetime, timedelta
from unittest import mock


def test_accepted_timestamp():
    mock_producer = mock.MagicMock()
    handler = BaseUpdateHandler(producer=mock_producer, pv_name="some_name", output_topic="topic_name")
    handler._publish_message(b"", datetime.now().timestamp()*1e9)
    assert mock_producer.produce.called


def test_ordered_timestamps():
    mock_producer = mock.MagicMock()
    handler = BaseUpdateHandler(producer=mock_producer, pv_name="some_name", output_topic="topic_name")
    handler._publish_message(b"", datetime.now().timestamp()*1e9)
    handler._publish_message(b"", (datetime.now() + timedelta(seconds=1)).timestamp() * 1e9)
    assert mock_producer.produce.call_count == 2


def test_too_old_timestamp():
    mock_producer = mock.MagicMock()
    handler = BaseUpdateHandler(producer=mock_producer, pv_name="some_name", output_topic="topic_name")
    handler._publish_message(b"", (datetime.now() - timedelta(days=400)).timestamp()*1e9)
    assert not mock_producer.produce.called


def test_too_futuristic_timestamp():
    mock_producer = mock.MagicMock()
    handler = BaseUpdateHandler(producer=mock_producer, pv_name="some_name", output_topic="topic_name")
    handler._publish_message(b"", (datetime.now() + timedelta(minutes=60)).timestamp()*1e9)
    assert not mock_producer.produce.called


def test_out_of_order_timestamps():
    mock_producer = mock.MagicMock()
    handler = BaseUpdateHandler(producer=mock_producer, pv_name="some_name", output_topic="topic_name")
    handler._publish_message(b"", datetime.now().timestamp()*1e9)
    handler._publish_message(b"", (datetime.now() - timedelta(seconds=1)).timestamp() * 1e9)
    assert mock_producer.produce.call_count == 1

