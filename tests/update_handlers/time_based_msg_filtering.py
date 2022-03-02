from datetime import datetime, timedelta
from unittest import mock

from forwarder.update_handlers.base_update_handler import SerialiserTracker


def test_accepted_timestamp():
    mock_publisher = mock.MagicMock()
    mock_serialiser = mock.MagicMock()
    handler = SerialiserTracker(serialiser=mock_serialiser, publisher=mock_publisher)
    handler.set_new_message(b"", datetime.now().timestamp() * 1e9)
    assert mock_publisher.publish_message.called


def test_ordered_timestamps():
    mock_publisher = mock.MagicMock()
    mock_serialiser = mock.MagicMock()
    handler = SerialiserTracker(serialiser=mock_serialiser, publisher=mock_publisher)
    handler.set_new_message(b"", datetime.now().timestamp() * 1e9)
    handler.set_new_message(
        b"", (datetime.now() + timedelta(seconds=1)).timestamp() * 1e9
    )
    assert mock_publisher.publish_message.call_count == 2


def test_too_old_timestamp():
    mock_publisher = mock.MagicMock()
    mock_serialiser = mock.MagicMock()
    handler = SerialiserTracker(serialiser=mock_serialiser, publisher=mock_publisher)
    handler.set_new_message(
        b"", (datetime.now() - timedelta(days=400)).timestamp() * 1e9
    )
    assert not mock_publisher.publish_message.called


def test_too_futuristic_timestamp():
    mock_publisher = mock.MagicMock()
    mock_serialiser = mock.MagicMock()
    handler = SerialiserTracker(serialiser=mock_serialiser, publisher=mock_publisher)
    handler.set_new_message(
        b"", (datetime.now() + timedelta(minutes=60)).timestamp() * 1e9
    )
    assert not mock_publisher.publish_message.called


def test_out_of_order_timestamps():
    mock_publisher = mock.MagicMock()
    mock_serialiser = mock.MagicMock()
    handler = SerialiserTracker(serialiser=mock_serialiser, publisher=mock_publisher)
    handler.set_new_message(b"", datetime.now().timestamp() * 1e9)
    handler.set_new_message(
        b"", (datetime.now() - timedelta(seconds=1)).timestamp() * 1e9
    )
    assert mock_publisher.publish_message.call_count == 1
