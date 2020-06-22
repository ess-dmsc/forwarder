from tests.kafka.fake_producer import FakeProducer
from tests.test_helpers.p4p_fakes import FakeContext
from forwarder.update_handlers.pva_update_handler import PVAUpdateHandler
from p4p.nt import NTScalar, NTEnum
from streaming_data_types.logdata_f142 import deserialise_f142
from cmath import isclose
import pytest


def test_update_handler_publishes_enum_update():
    producer = FakeProducer()
    context = FakeContext()

    pv_index = 0
    pv_value_str = "choice0"
    pv_timestamp_s = 1.1  # seconds from unix epoch
    pv_source_name = "source_name"

    pva_update_handler = PVAUpdateHandler(producer, context, pv_source_name, "output_topic", "f142")  # type: ignore
    context.call_monitor_callback_with_fake_pv_update(
        NTEnum(valueAlarm=True).wrap(
            {"index": pv_index, "choices": [pv_value_str, "choice1", "choice2"]},
            timestamp=pv_timestamp_s,
        )
    )

    assert producer.published_payload is not None
    pv_update_output = deserialise_f142(producer.published_payload)
    assert pv_update_output.value == pv_value_str
    assert pv_update_output.source_name == pv_source_name

    pva_update_handler.stop()


@pytest.mark.parametrize("pv_value,pv_type", [(4.2222, "d"), (4.2, "f")])
def test_update_handler_publishes_float_update(pv_value, pv_type):
    producer = FakeProducer()
    context = FakeContext()

    pv_timestamp_s = 1.1  # seconds from unix epoch
    pv_source_name = "source_name"

    pva_update_handler = PVAUpdateHandler(producer, context, pv_source_name, "output_topic", "f142")  # type: ignore
    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(pv_value, timestamp=pv_timestamp_s)
    )

    assert producer.published_payload is not None
    pv_update_output = deserialise_f142(producer.published_payload)
    assert isclose(pv_update_output.value, pv_value, abs_tol=0.0001)
    assert pv_update_output.source_name == pv_source_name

    pva_update_handler.stop()


@pytest.mark.parametrize(
    "pv_value,pv_type",
    [(1, "l"), (2, "L"), (-3, "i"), (4, "I"), (-5, "h"), (6, "H"), (-7, "b"), (8, "B")],
)
def test_update_handler_publishes_int_update(pv_value, pv_type):
    producer = FakeProducer()
    context = FakeContext()

    pv_timestamp_s = 1.1  # seconds from unix epoch
    pv_source_name = "source_name"

    pva_update_handler = PVAUpdateHandler(producer, context, pv_source_name, "output_topic", "f142")  # type: ignore
    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(pv_value, timestamp=pv_timestamp_s)
    )

    assert producer.published_payload is not None
    pv_update_output = deserialise_f142(producer.published_payload)
    assert pv_update_output.value == pv_value
    assert pv_update_output.source_name == pv_source_name

    pva_update_handler.stop()
