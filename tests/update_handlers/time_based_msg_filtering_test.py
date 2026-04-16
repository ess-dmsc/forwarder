from datetime import datetime, timedelta
from unittest import mock

from forwarder.kafka.kafka_producer import KafkaProducer
from forwarder.update_handlers.serialiser_tracker import SerialiserTracker


def create_handler(reject_older_timestamps=True):
    mock_producer = mock.MagicMock(spec=KafkaProducer)
    mock_serialiser = mock.MagicMock()
    mock_serialiser.reject_older_timestamps.return_value = reject_older_timestamps
    handler = SerialiserTracker(
        mock_serialiser, mock_producer, "::SOME_PV::", "::SOME_TOPIC::"
    )
    return mock_producer, handler


def test_accepted_timestamp():
    mock_producer, handler = create_handler()

    handler.set_new_message(b"", datetime.now().timestamp() * 1e9)

    assert mock_producer.produce.called
    handler.stop()


def test_ordered_timestamps():
    mock_producer, handler = create_handler()

    handler.set_new_message(b"", datetime.now().timestamp() * 1e9)
    handler.set_new_message(
        b"", (datetime.now() + timedelta(seconds=1)).timestamp() * 1e9
    )

    assert mock_producer.produce.call_count == 2
    handler.stop()


def test_too_old_timestamp():
    mock_producer, handler = create_handler()

    handler.set_new_message(
        b"", (datetime.now() - timedelta(days=400)).timestamp() * 1e9
    )

    assert not mock_producer.produce.called
    handler.stop()


def test_too_futuristic_timestamp():
    mock_producer, handler = create_handler()

    handler.set_new_message(
        b"", (datetime.now() + timedelta(minutes=60)).timestamp() * 1e9
    )

    assert not mock_producer.produce.called
    handler.stop()


def test_out_of_order_timestamps_ignored():
    mock_producer, handler = create_handler()

    handler.set_new_message(b"", datetime.now().timestamp() * 1e9)
    handler.set_new_message(
        b"", (datetime.now() - timedelta(seconds=1)).timestamp() * 1e9
    )
    handler.set_new_message(b"", datetime.now().timestamp() * 1e9)

    assert mock_producer.produce.call_count == 2
    handler.stop()


def test_out_of_order_timestamps_published_if_serialiser_allows_it():
    mock_producer, handler = create_handler(reject_older_timestamps=False)

    handler.set_new_message(b"", datetime.now().timestamp() * 1e9)
    handler.set_new_message(
        b"", (datetime.now() - timedelta(seconds=1)).timestamp() * 1e9
    )

    assert mock_producer.produce.call_count == 2
    handler.stop()
