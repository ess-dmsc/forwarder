from tests.kafka.fake_producer import FakeProducer
from forwarder.update_handlers.ca_update_handler import CAUpdateHandler
from tests.test_helpers.ca_fakes import FakeContext
from cmath import isclose
from streaming_data_types.logdata_f142 import deserialise_f142
from streaming_data_types.timestamps_tdct import deserialise_tdct
import pytest
from caproto import ReadNotifyResponse, ChannelType, TimeStamp
import numpy as np
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import AlarmSeverity
from time import sleep
from typing import List


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


def test_update_handler_does_not_include_alarm_details_if_unchanged_in_subsequent_updates():
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
    # Second update, with unchanged alarm
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

    assert producer.messages_published == 2
    pv_update_output = deserialise_f142(producer.published_payload)
    assert pv_update_output.alarm_status == AlarmStatus.NO_CHANGE
    assert pv_update_output.alarm_severity == AlarmSeverity.NO_CHANGE

    update_handler.stop()


def test_update_handler_publishes_enum_update():
    producer = FakeProducer()
    context = FakeContext()

    pv_caproto_type = ChannelType.TIME_ENUM
    pv_source_name = "source_name"
    update_handler = CAUpdateHandler(producer, context, pv_source_name, "output_topic", "f142")  # type: ignore

    # Nothing gets published when ENUM type update is received, the handler will resubscribe using STRING
    # type as the string is more useful to forwarder to the filewriter than the enum int
    metadata = (0, 0, TimeStamp(4, 0))
    context.call_monitor_callback_with_fake_pv_update(
        ReadNotifyResponse(np.array([0]), pv_caproto_type, 1, 1, 1, metadata=metadata,)
    )
    # Second update, with STRING type
    enum_string_value = "ENUM_STRING"
    context.call_monitor_callback_with_fake_pv_update(
        ReadNotifyResponse(
            [enum_string_value.encode("utf8")],
            ChannelType.TIME_STRING,
            1,
            1,
            1,
            metadata=metadata,
        )
    )

    assert (
        producer.messages_published == 1
    ), "Only expected a single message with string payload, not the original enum update"
    assert producer.published_payload is not None
    pv_update_output = deserialise_f142(producer.published_payload)
    assert pv_update_output.value == enum_string_value
    assert pv_update_output.source_name == pv_source_name

    update_handler.stop()


def test_empty_update_is_not_forwarded():
    producer = FakeProducer()
    context = FakeContext()

    pv_value = [1, 2, 3]
    pv_caproto_type = ChannelType.TIME_INT
    pv_numpy_type = np.int32
    pv_source_name = "chopper"

    update_handler = CAUpdateHandler(producer, context, pv_source_name, "output_topic", "tdct")  # type: ignore
    metadata = (0, 0, TimeStamp(4, 0))

    # First update, with non-empty timestamp array
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

    # Second update, with empty timestamp array
    # We think empty array PV updates may occur for chopper timestamp PVs when the chopper is not rotating
    empty_pv_value: List = []
    context.call_monitor_callback_with_fake_pv_update(
        ReadNotifyResponse(
            np.array([empty_pv_value]).astype(pv_numpy_type),
            pv_caproto_type,
            1,
            1,
            1,
            metadata=metadata,
        )
    )

    assert (
        producer.messages_published == 1
    ), "Expected only the one PV update with non-empty value array to have been published"
    pv_update_output = deserialise_tdct(producer.published_payload)
    assert (
        pv_update_output.timestamps
    ), "Expected the published PV update not to be empty"

    update_handler.stop()


def test_empty_update_is_not_cached():
    producer = FakeProducer()
    context = FakeContext()

    # We think empty array PV updates may occur for chopper timestamp PVs when the chopper is not rotating
    empty_pv_value: List = []
    pv_caproto_type = ChannelType.TIME_INT
    pv_numpy_type = np.int32
    pv_source_name = "chopper"

    # Set an update period to enable PV update caching, so that we can test
    auto_update_period = 5000
    update_handler = CAUpdateHandler(producer, context, pv_source_name, "output_topic", "tdct", auto_update_period)  # type: ignore
    metadata = (0, 0, TimeStamp(4, 0))

    # Update with empty timestamp array
    context.call_monitor_callback_with_fake_pv_update(
        ReadNotifyResponse(
            np.array([empty_pv_value]).astype(pv_numpy_type),
            pv_caproto_type,
            1,
            1,
            1,
            metadata=metadata,
        )
    )

    assert (
        update_handler._cached_update is None
    ), "Expected the empty update not to have been cached"

    update_handler.stop()
