from tests.kafka.fake_producer import FakeProducer
from forwarder.update_handlers.fake_update_handler import FakeUpdateHandler
from streaming_data_types.logdata_f142 import deserialise_f142
import pytest


def test_update_handler_throws_if_schema_not_recognised():
    producer = FakeProducer()
    non_existing_schema = "DOESNTEXIST"
    with pytest.raises(ValueError):
        FakeUpdateHandler(producer, "source_name", "output_topic", non_existing_schema, 20000)  # type: ignore


def test_update_handler_publishes_f142_update():
    producer = FakeProducer()

    pv_source_name = "source_name"
    fake_update_handler = FakeUpdateHandler(producer, pv_source_name, "output_topic", "f142", 100)  # type: ignore
    fake_update_handler._timer_callback()

    assert producer.published_payload is not None
    pv_update_output = deserialise_f142(producer.published_payload)
    assert pv_update_output.source_name == pv_source_name

    fake_update_handler.stop()
