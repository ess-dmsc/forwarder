import time
from time import sleep
from typing import List

import numpy as np
import pytest
from caproto import (
    AlarmStatus,
    ChannelType,
    ReadNotifyResponse,
    TimeStamp,
    timestamp_to_epics,
)
from streaming_data_types.alarm_al00 import Severity as al00_Severity
from streaming_data_types.alarm_al00 import deserialise_al00
from streaming_data_types.epics_connection_ep01 import ConnectionInfo, deserialise_ep01
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import (
    AlarmSeverity as f142_AlarmSeverity,
)
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import (
    AlarmStatus as f142_AlarmStatus,
)
from streaming_data_types.logdata_f142 import deserialise_f142
from streaming_data_types.logdata_f144 import deserialise_f144
from streaming_data_types.timestamps_tdct import deserialise_tdct

from forwarder.common import EpicsProtocol
from forwarder.update_handlers.ca_update_handler import CAUpdateHandler
from forwarder.update_handlers.serialiser_tracker import create_serialiser_list
from tests.kafka.fake_producer import FakeProducer
from tests.test_helpers.ca_fakes import FakeContext

# Set the epics protocol for all tests in this module.
# The epics_protocol mark is used by the context creation fixture.
pytestmark = pytest.mark.epics_protocol(EpicsProtocol.CA)


def epics_timestamp():
    return timestamp_to_epics(time.time())


def test_update_handler_throws_if_schema_not_recognised():
    producer = FakeProducer()
    context = FakeContext()
    non_existing_schema = "DOESNTEXIST"
    with pytest.raises(ValueError):
        CAUpdateHandler(
            context,
            "source_name",
            create_serialiser_list(
                producer,  # type: ignore
                "source_name",
                "output_topic",
                non_existing_schema,
                EpicsProtocol.CA,
            ),
        )


@pytest.mark.schema("f142")
@pytest.mark.parametrize(
    "pv_value,pv_caproto_type,pv_numpy_type",
    [
        # float
        (4.2222, ChannelType.TIME_DOUBLE, np.dtype("float64")),
        (4.2, ChannelType.TIME_FLOAT, np.dtype("float32")),
        # int
        (1, ChannelType.TIME_LONG, np.int64),
        (-3, ChannelType.TIME_INT, np.int32),
        # floatarray
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
        # enum
        (0, ChannelType.TIME_ENUM, np.dtype("int32")),
    ],
)
def test_update_handler_publishes_f142_update(
    pv_value, pv_caproto_type, pv_numpy_type, context, producer, pv_source_name
):
    data_count = len(pv_value) if isinstance(pv_value, np.ndarray) else 1
    metadata = (0, 0, TimeStamp(*epics_timestamp()))
    context.call_monitor_callback_with_fake_pv_update(
        ReadNotifyResponse(
            np.array([pv_value]).astype(pv_numpy_type),
            pv_caproto_type,
            data_count,
            1,
            1,
            metadata=metadata,
        )
    )

    assert len(producer.published_payloads) == 1
    pv_update_output = deserialise_f142(producer.published_payloads[-1])
    assert np.allclose(pv_update_output.value, pv_value)
    assert pv_update_output.source_name == pv_source_name
    # assert pv_update_output.alarm_severity == f142_AlarmStatus.WRITE
    # assert pv_update_output.alarm_status == f142_AlarmSeverity.MINOR


@pytest.mark.schema("f144")
@pytest.mark.parametrize(
    "pv_value,pv_caproto_type,pv_numpy_type",
    [
        # float
        (4.2222, ChannelType.TIME_DOUBLE, np.dtype("float64")),
        (4.2, ChannelType.TIME_FLOAT, np.dtype("float32")),
        # int
        (1, ChannelType.TIME_LONG, np.int64),
        (-3, ChannelType.TIME_INT, np.int32),
        # floatarray
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
        # enum
        (0, ChannelType.TIME_ENUM, np.dtype("int32")),
    ],
)
def test_update_handler_publishes_f144_update(
    pv_value, pv_caproto_type, pv_numpy_type, context, producer, pv_source_name
):
    data_count = len(pv_value) if isinstance(pv_value, np.ndarray) else 1
    metadata = (0, 0, TimeStamp(*epics_timestamp()))
    context.call_monitor_callback_with_fake_pv_update(
        ReadNotifyResponse(
            np.array([pv_value]).astype(pv_numpy_type),
            pv_caproto_type,
            data_count,
            1,
            1,
            metadata=metadata,
        )
    )

    # The assertions below assume that the f144 message was sent before the al00 one.
    assert len(producer.published_payloads) == 2
    pv_update_output = deserialise_f144(producer.published_payloads[-2])
    assert np.allclose(pv_update_output.value, pv_value)
    assert pv_update_output.source_name == pv_source_name
    pv_update_alarm_output = deserialise_al00(producer.published_payloads[-1])
    assert pv_update_alarm_output.source == pv_source_name
    assert pv_update_alarm_output.severity == al00_Severity.OK
    assert pv_update_alarm_output.message == "NO_ALARM"


@pytest.mark.schema("f142")
def test_update_handler_publishes_f142_alarm_update(context, producer, pv_source_name):
    pv_value = 42
    pv_caproto_type = ChannelType.TIME_INT
    pv_numpy_type = np.int32
    alarm_status = 6  # AlarmStatus.LOW
    alarm_severity = 1  # AlarmSeverity.MINOR

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

    assert len(producer.published_payloads) == 1
    pv_update_output = deserialise_f142(producer.published_payloads[-1])
    assert pv_update_output.source_name == pv_source_name
    assert pv_update_output.value == pv_value
    assert pv_update_output.alarm_status == f142_AlarmStatus.LOW
    assert pv_update_output.alarm_severity == f142_AlarmSeverity.MINOR


@pytest.mark.schema("f144")
def test_update_handler_publishes_f144_alarm_update(context, producer, pv_source_name):
    pv_value = 42
    pv_caproto_type = ChannelType.TIME_INT
    pv_numpy_type = np.int32
    alarm_status = 6  # AlarmStatus.LOW
    alarm_severity = 1  # AlarmSeverity.MINOR

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

    # The assertions below assume that the f144 message was sent before the al00 one.
    assert len(producer.published_payloads) == 2
    pv_update_output = deserialise_al00(producer.published_payloads[-1])
    assert pv_update_output.source == pv_source_name
    assert pv_update_output.message == AlarmStatus(alarm_status).name
    assert pv_update_output.severity == al00_Severity.MINOR


@pytest.mark.schema("f142")
@pytest.mark.serialiser_update_period_ms(10)
def test_update_handler_publishes_periodic_update_f142(
    context, producer, pv_source_name
):
    pv_value = 42
    pv_caproto_type = ChannelType.TIME_INT
    pv_numpy_type = np.int32

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

    assert len(producer.published_payloads) == 1
    pv_update_output = deserialise_f142(producer.published_payloads[-1])
    assert pv_update_output.value == pv_value
    assert pv_update_output.source_name == pv_source_name

    sleep(0.05)
    assert (
        producer.messages_published > 1
    ), "Expected more than the 1 message from triggered update due to periodic updates being active"


@pytest.mark.schema("f144")
@pytest.mark.serialiser_update_period_ms(10)
def test_update_handler_publishes_periodic_update_f144(
    context, producer, pv_source_name
):
    pv_value = 44
    pv_caproto_type = ChannelType.TIME_INT
    pv_numpy_type = np.int32

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

    # The assertions below assume that the f144 message was sent before the al00 one.
    assert len(producer.published_payloads) == 2
    pv_update_output = deserialise_f144(producer.published_payloads[-2])
    assert pv_update_output.value == pv_value
    assert pv_update_output.source_name == pv_source_name

    sleep(0.05)
    assert (
        producer.messages_published > 2
    ), "Expected more than the 1 message from triggered update due to periodic updates being active"


@pytest.mark.schema("f142")
def test_update_handler_always_includes_alarm_status_f142(context, producer):
    pv_value = 42
    pv_caproto_type = ChannelType.TIME_INT
    pv_numpy_type = np.int32
    alarm_status = 6  # f142_AlarmStatus.LOW
    alarm_severity = 1  # al00_Severity.MINOR f142_AlarmSeverity.MINOR

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
    pv_update_output = deserialise_f142(producer.published_payloads[-1])
    assert pv_update_output.alarm_severity == f142_AlarmSeverity.MINOR
    assert pv_update_output.alarm_status == f142_AlarmStatus.LOW


@pytest.mark.schema("f144")
def test_update_handler_always_includes_alarm_status_f144(context, producer):
    pv_value = 44
    pv_caproto_type = ChannelType.TIME_INT
    pv_numpy_type = np.int32
    alarm_status = 6  # f142_AlarmStatus.LOW
    alarm_severity = 1  # al00_Severity.MINOR f142_AlarmSeverity.MINOR

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

    assert producer.messages_published == 4
    pv_update_output = deserialise_al00(producer.published_payloads[-1])
    assert pv_update_output.severity == al00_Severity.MINOR
    assert pv_update_output.message == "LOW"


@pytest.mark.schema("tdct")
def test_empty_update_is_not_forwarded(context, producer, pv_source_name):
    pv_value = [1, 2, 3]
    pv_caproto_type = ChannelType.TIME_INT
    pv_numpy_type = np.int32

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
    pv_update_output = deserialise_tdct(producer.published_payloads[-1])
    assert (
        pv_update_output.timestamps.size > 0
    ), "Expected the published PV update not to be empty"


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
    try:
        update_handler = CAUpdateHandler(
            context,
            pv_source_name,
            create_serialiser_list(producer, pv_source_name, "output_topic", "tdct", EpicsProtocol.CA, auto_update_period),  # type: ignore
        )
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
            update_handler.serialiser_tracker_list[0]._cached_update is None
        ), "Expected the empty update not to have been cached"
    finally:
        update_handler.stop()


@pytest.mark.schema("f144")
@pytest.mark.parametrize(
    "state_string,state_enum",
    [
        ("connected", ConnectionInfo.CONNECTED),
        ("disconnected", ConnectionInfo.DISCONNECTED),
        ("destroyed", ConnectionInfo.DESTROYED),
        ("cancelled", ConnectionInfo.CANCELLED),
        ("finished", ConnectionInfo.FINISHED),
        ("remote_error", ConnectionInfo.REMOTE_ERROR),
        ("some_unrecognised", ConnectionInfo.UNKNOWN),
    ],
)
def test_handler_publishes_connection_state_change(
    state_string, state_enum, context, producer, pv_source_name
):
    context.call_connection_state_callback_with_fake_state_change(state_string)

    assert len(producer.published_payloads) > 0
    connect_state_output = deserialise_ep01(producer.published_payloads[-1])
    assert connect_state_output.status == state_enum
    assert connect_state_output.source_name == pv_source_name
