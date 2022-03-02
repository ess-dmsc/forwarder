import pytest
from streaming_data_types.logdata_f142 import deserialise_f142
from streaming_data_types.timestamps_tdct import deserialise_tdct

from forwarder.update_handlers.fake_update_handler import FakeUpdateHandler
from tests.kafka.fake_producer import FakeProducer


def test_update_handler_throws_if_schema_not_recognised():
    producer = FakeProducer()
    non_existing_schema = "DOESNTEXIST"
    with pytest.raises(ValueError):
        FakeUpdateHandler(producer, "source_name", "output_topic", non_existing_schema, 20000)  # type: ignore


def test_update_handler_publishes_update():
    got_f142: bool = False
    pv_name: str = ""

    def check_payload(payload):
        try:
            deserialized = deserialise_f142(payload)
            nonlocal pv_name, got_f142
            pv_name = deserialized.source_name
            got_f142 = True
        except Exception:
            pass

    producer = FakeProducer(check_payload)

    pv_source_name = "source_name"
    fake_update_handler = FakeUpdateHandler(producer, pv_source_name, "output_topic", "f142", 100)  # type: ignore
    fake_update_handler._timer_callback()

    assert got_f142
    assert pv_name == pv_source_name

    fake_update_handler.stop()


def test_update_handler_publishes_tdct_update():
    got_tdct: bool = False
    pv_name: str = ""

    def check_payload(payload):
        try:
            deserialized = deserialise_tdct(payload)
            nonlocal pv_name, got_tdct
            pv_name = deserialized.name
            got_tdct = True
        except Exception:
            pass

    producer = FakeProducer(check_payload)
    pv_source_name = "source_name"
    fake_update_handler = FakeUpdateHandler(producer, pv_source_name, "output_topic", "tdct", 100)  # type: ignore
    fake_update_handler._timer_callback()

    assert got_tdct
    assert pv_name == pv_source_name

    fake_update_handler.stop()
