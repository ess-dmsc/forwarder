from tests.kafka.fake_producer import FakeProducer
from tests.test_helpers.p4p_fakes import FakeContext
from forwarder.update_handlers.pva_update_handler import PVAUpdateHandler
from p4p.nt import NTScalar, NTEnum
from streaming_data_types.logdata_f142 import deserialise_f142
from cmath import isclose
import pytest
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import AlarmSeverity


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


def test_update_handler_publishes_alarm_update():
    producer = FakeProducer()
    context = FakeContext()

    pv_value = 42
    pv_type = "i"
    pv_timestamp_s = 1.1  # seconds from unix epoch
    pv_source_name = "source_name"
    alarm_status = 4  # Indicates RECORD alarm, we map the alarm message to a specific alarm status to forward
    alarm_severity = 1  # AlarmSeverity.MINOR
    alarm_message = "HIGH_ALARM"

    pva_update_handler = PVAUpdateHandler(producer, context, pv_source_name, "output_topic", "f142")  # type: ignore
    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(
            {
                "value": pv_value,
                "alarm": {
                    "status": alarm_status,
                    "severity": alarm_severity,
                    "message": alarm_message,
                },
            },
            timestamp=pv_timestamp_s,
        )
    )

    assert producer.published_payload is not None
    pv_update_output = deserialise_f142(producer.published_payload)
    assert pv_update_output.value == pv_value
    assert pv_update_output.source_name == pv_source_name
    assert pv_update_output.alarm_status == AlarmStatus.HIGH
    assert pv_update_output.alarm_severity == AlarmSeverity.MINOR

    pva_update_handler.stop()
