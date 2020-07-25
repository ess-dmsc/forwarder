from tests.kafka.fake_producer import FakeProducer
from forwarder.update_handlers.ca_update_handler import CAUpdateHandler
from tests.test_helpers.ca_fakes import FakeContext
from cmath import isclose
from streaming_data_types.logdata_f142 import deserialise_f142
import pytest
from caproto import ReadNotifyResponse, ChannelType, TimeStamp
import numpy as np
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import AlarmSeverity
from time import sleep


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


@pytest.mark.parametrize(
    "pv_value,pv_caproto_type,pv_numpy_type",
    [(1, ChannelType.TIME_LONG, np.int64), (-3, ChannelType.TIME_INT, np.int32)],
)
def test_update_handler_publishes_int_update(pv_value, pv_caproto_type, pv_numpy_type):
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
    assert pv_update_output.value == pv_value
    assert pv_update_output.source_name == pv_source_name

    update_handler.stop()


@pytest.mark.parametrize(
    "pv_value,pv_caproto_type,pv_numpy_type",
    [
        (
            np.array([1.1, 2.2, 3.3]).astype(np.float64),
            ChannelType.TIME_DOUBLE,
            np.float64,
        ),
        (
            np.array([1.1, 2.2, 3.3]).astype(np.float32),
            ChannelType.TIME_FLOAT,
            np.float32,
        ),
    ],
)
def test_update_handler_publishes_floatarray_update(
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
            len(pv_value),
            1,
            1,
            metadata=metadata,
        )
    )

    assert producer.published_payload is not None
    pv_update_output = deserialise_f142(producer.published_payload)
    assert np.allclose(pv_update_output.value, pv_value)
    assert pv_update_output.source_name == pv_source_name

    update_handler.stop()


def test_update_handler_publishes_alarm_update():
    producer = FakeProducer()
    context = FakeContext()

    pv_value = 42
    pv_caproto_type = ChannelType.TIME_INT
    pv_numpy_type = np.int32
    pv_source_name = "source_name"
    alarm_status = 6  # AlarmStatus.LOW
    alarm_severity = 1  # AlarmSeverity.MINOR

    update_handler = CAUpdateHandler(producer, context, pv_source_name, "output_topic", "f142")  # type: ignore

    metadata = (alarm_status, alarm_severity, TimeStamp(4, 0))
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
    assert pv_update_output.value == pv_value
    assert pv_update_output.source_name == pv_source_name
    assert pv_update_output.alarm_status == AlarmStatus.LOW
    assert pv_update_output.alarm_severity == AlarmSeverity.MINOR

    update_handler.stop()


def test_update_handler_publishes_periodic_update():
    producer = FakeProducer()
    context = FakeContext()

    pv_value = 42
    pv_caproto_type = ChannelType.TIME_INT
    pv_numpy_type = np.int32
    pv_source_name = "source_name"

    update_period_ms = 10
    update_handler = CAUpdateHandler(producer, context, pv_source_name, "output_topic", "f142", update_period_ms)  # type: ignore
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
    assert pv_update_output.value == pv_value
    assert pv_update_output.source_name == pv_source_name

    sleep(0.05)
    assert (
        producer.messages_published > 1
    ), "Expected more than the 1 message from triggered update due to periodic updates being active"

    update_handler.stop()


# def test_update_handler_does_not_include_alarm_details_if_unchanged_in_subsequent_updates():
#     producer = FakeProducer()
#     context = FakeContext()
#
#     pv_timestamp_s = 1.1  # seconds from unix epoch
#     pv_source_name = "source_name"
#     pv_value = -3
#     pv_type = "i"
#     alarm_status = 4  # Indicates RECORD alarm, we map the alarm message to a specific alarm status to forward
#     alarm_severity = 1  # AlarmSeverity.MINOR
#     alarm_message = "HIGH_ALARM"
#
#     pva_update_handler = PVAUpdateHandler(producer, context, pv_source_name, "output_topic", "f142")  # type: ignore
#     context.call_monitor_callback_with_fake_pv_update(
#         NTScalar(pv_type, valueAlarm=True).wrap(
#             {
#                 "value": pv_value,
#                 "alarm": {
#                     "status": alarm_status,
#                     "severity": alarm_severity,
#                     "message": alarm_message,
#                 },
#             },
#             timestamp=pv_timestamp_s,
#         )
#     )
#     # Second update, with unchanged alarm
#     context.call_monitor_callback_with_fake_pv_update(
#         NTScalar(pv_type, valueAlarm=True).wrap(
#             {
#                 "value": pv_value,
#                 "alarm": {
#                     "status": alarm_status,
#                     "severity": alarm_severity,
#                     "message": alarm_message,
#                 },
#             },
#             timestamp=pv_timestamp_s,
#         )
#     )
#
#     assert producer.messages_published == 2
#     pv_update_output = deserialise_f142(producer.published_payload)
#     assert pv_update_output.alarm_status == AlarmStatus.NO_CHANGE
#     assert pv_update_output.alarm_severity == AlarmSeverity.NO_CHANGE
#
#     pva_update_handler.stop()
