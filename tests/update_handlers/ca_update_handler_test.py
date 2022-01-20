import time
from cmath import isclose
from time import sleep
from typing import List

import numpy as np
import pytest
from caproto import ChannelType, ReadNotifyResponse, TimeStamp, timestamp_to_epics
from streaming_data_types.epics_connection_info_ep00 import deserialise_ep00
from streaming_data_types.fbschemas.epics_connection_info_ep00.EventType import (
    EventType as ConnectionEventType,
)
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import AlarmSeverity
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
from streaming_data_types.logdata_f142 import deserialise_f142
from streaming_data_types.timestamps_tdct import deserialise_tdct

from forwarder.update_handlers.ca_update_handler import CAUpdateHandler
from tests.kafka.fake_producer import FakeProducer
from tests.test_helpers.ca_fakes import FakeContext


def epics_timestamp():
    return timestamp_to_epics(time.time())


def test_update_handler_throws_if_schema_not_recognised():
    producer = FakeProducer()
    context = FakeContext()
    non_existing_schema = "DOESNTEXIST"
    with pytest.raises(ValueError):
        CAUpdateHandler(producer, context, "source_name", "output_topic", non_existing_schema)  # type: ignore


@pytest.mark.parametrize(
    "pv_value,pv_caproto_type,pv_numpy_type",
    [
        (4.2222, ChannelType.TIME_DOUBLE, np.dtype("float64")),
        (4.2, ChannelType.TIME_FLOAT, np.dtype("float32")),
    ],
)
def test_update_handler_publishes_float_update(
    pv_value, pv_caproto_type, pv_numpy_type
):
    producer = FakeProducer()
    context = FakeContext()

    pv_source_name = "source_name"
    update_handler = CAUpdateHandler(producer, context, pv_source_name, "output_topic", "f142")  # type: ignore
    metadata = (0, 0, TimeStamp(*epics_timestamp()))
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

    metadata = (0, 0, TimeStamp(*epics_timestamp()))
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

    metadata = (0, 0, TimeStamp(*epics_timestamp()))
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

    metadata = (alarm_status, alarm_severity, TimeStamp(*epics_timestamp()))
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
    metadata = (0, 0, TimeStamp(*epics_timestamp()))
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
    metadata = (alarm_status, alarm_severity, TimeStamp(*epics_timestamp()))
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
    metadata = (0, 0, TimeStamp(*epics_timestamp()))
    context.call_monitor_callback_with_fake_pv_update(
        ReadNotifyResponse(
            np.array([0]),
            pv_caproto_type,
            1,
            1,
            1,
            metadata=metadata,
        )
    )

    assert producer.messages_published == 1
    assert producer.published_payload is not None
    pv_update_output = deserialise_f142(producer.published_payload)
    assert pv_update_output.value == 0
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
    metadata = (0, 0, TimeStamp(*epics_timestamp()))

    # First update, with non-empty timestamp array
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
        pv_update_output.timestamps.size > 0
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
    metadata = (0, 0, TimeStamp(*epics_timestamp()))

    # Update with empty timestamp array
    context.call_monitor_callback_with_fake_pv_update(
        ReadNotifyResponse(
            np.array([empty_pv_value]).astype(pv_numpy_type),
            pv_caproto_type,
            len(empty_pv_value),
            1,
            1,
            metadata=metadata,
        )
    )

    assert (
        update_handler._cached_update is None
    ), "Expected the empty update not to have been cached"

    update_handler.stop()


@pytest.mark.parametrize(
    "state_string,state_enum",
    [
        ("connected", ConnectionEventType.CONNECTED),
        ("disconnected", ConnectionEventType.DISCONNECTED),
        ("destroyed", ConnectionEventType.DESTROYED),
        ("some_unrecognised", ConnectionEventType.UNKNOWN),
    ],
)
def test_handler_publishes_connection_state_change(state_string, state_enum):
    producer = FakeProducer()
    context = FakeContext()

    pv_source_name = "source_name"
    update_handler = CAUpdateHandler(producer, context, pv_source_name, "output_topic", "f142")  # type: ignore

    context.call_connection_state_callback_with_fake_state_change(state_string)

    assert producer.published_payload is not None
    connect_state_output = deserialise_ep00(producer.published_payload)
    assert connect_state_output.type == state_enum
    assert connect_state_output.source_name == pv_source_name

    update_handler.stop()
