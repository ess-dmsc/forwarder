from tests.kafka.fake_producer import FakeProducer
from forwarder.update_handlers.ca_update_handler import CAUpdateHandler
from tests.test_helpers.ca_fakes import FakeContext
from cmath import isclose
from streaming_data_types.logdata_f142 import deserialise_f142
import pytest
from caproto import ReadNotifyResponse, ChannelType, TimeStamp
import numpy as np


def test_update_handler_throws_if_schema_not_recognised():
    producer = FakeProducer()
    context = FakeContext()
    non_existing_schema = "DOESNTEXIST"
    with pytest.raises(ValueError):
        CAUpdateHandler(producer, context, "source_name", "output_topic", non_existing_schema)  # type: ignore


@pytest.mark.parametrize(
    "pv_value,pv_caproto_type,pv_numpy_type",
    [
        (4.2222, ChannelType.TIME_DOUBLE, np.float64),
        (4.2, ChannelType.TIME_FLOAT, np.float32),
    ],
)
def test_update_handler_publishes_float_update(
    pv_value, pv_caproto_type, pv_numpy_type
):
    producer = FakeProducer()
    context = FakeContext()

    pv_source_name = "source_name"
    update_handler = CAUpdateHandler(producer, context, pv_source_name, "output_topic", "f142")  # type: ignore

    metadata = (0, 0, TimeStamp(4, 0))
    context.call_monitor_callback_with_fake_pv_update(
        ReadNotifyResponse(
            np.array([pv_value]).astype(pv_numpy_type),
            pv_caproto_type,
            1,
            1,
            1,
            metadata=metadata,
        )
    )

    assert producer.published_payload is not None
    pv_update_output = deserialise_f142(producer.published_payload)
    assert isclose(pv_update_output.value, pv_value, abs_tol=0.0001)
    assert pv_update_output.source_name == pv_source_name

    update_handler.stop()
