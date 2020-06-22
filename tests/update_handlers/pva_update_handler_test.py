from tests.kafka.fake_producer import FakeProducer
from tests.test_helpers.p4p_fakes import FakeContext
from forwarder.update_handlers.pva_update_handler import PVAUpdateHandler
from p4p import Value
from p4p.nt import NTScalar
from typing import Any
from streaming_data_types.logdata_f142 import deserialise_f142
from cmath import isclose


def create_scalar_pv_update(
    value: Any = 4.2, dtype: str = "d", timestamp_s: float = 1.1
) -> Value:
    return NTScalar(dtype, valueAlarm=True).wrap(value, timestamp=timestamp_s)


def test_update_handler_publishes_update():
    producer = FakeProducer()
    context = FakeContext()

    pv_value = 4.2
    pv_type = "d"  # double
    pv_timestamp_s = 1.1  # seconds from unix epoch
    pv_source_name = "source_name"

    pva_update_handler = PVAUpdateHandler(producer, context, pv_source_name, "output_topic", "f142")  # type: ignore
    context.call_monitor_callback_with_fake_pv_update(
        create_scalar_pv_update(pv_value, pv_type, pv_timestamp_s)
    )

    assert producer.published_payload is not None
    pv_update_output = deserialise_f142(producer.published_payload)
    assert isclose(pv_update_output.value, pv_value)
    assert pv_update_output.source_name == pv_source_name

    pva_update_handler.stop()
