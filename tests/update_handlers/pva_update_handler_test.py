from time import sleep, time
from typing import List

import numpy as np
import pytest
from p4p.client.thread import Cancelled, Disconnected, Finished, RemoteError
from p4p.nt import NTEnum, NTScalar
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
from streaming_data_types.utils import get_schema

from forwarder.common import EpicsProtocol
from forwarder.update_handlers.pva_update_handler import PVAUpdateHandler
from forwarder.update_handlers.serialiser_tracker import create_serialiser_list
from tests.kafka.fake_producer import FakeProducer
from tests.test_helpers.p4p_fakes import FakeContext

# Set the epics protocol for all tests in this module.
# The epics_protocol mark is used by the context creation fixture.
pytestmark = pytest.mark.epics_protocol(EpicsProtocol.PVA)


def test_update_handler_throws_if_schema_not_recognised():
    producer = FakeProducer()
    context = FakeContext()
    non_existing_schema = "DOESNTEXIST"
    with pytest.raises(KeyError):
        PVAUpdateHandler(
            context,
            "source_name",
            create_serialiser_list(
                producer,  # type: ignore
                "source_name",
                "output_topic",
                non_existing_schema,
                EpicsProtocol.PVA,
            ),
        )


@pytest.mark.schema("f142")
def test_update_handler_publishes_enum_update_f142(context, producer, pv_source_name):
    pv_index = 0
    pv_value_str = f"choice{pv_index}"
    pv_timestamp_s = time()  # seconds from unix epoch

    context.call_monitor_callback_with_fake_pv_update(
        NTEnum(valueAlarm=True).wrap(
            {"index": pv_index, "choices": [pv_value_str, "choice1", "choice2"]},
            timestamp=pv_timestamp_s,
        )
    )

    # The assertions below assume that an ep01 message is sent after the f142
    assert len(producer.published_payloads) == 2
    pv_update_output = deserialise_f142(producer.published_payloads[-2])
    assert np.allclose(pv_update_output.value, pv_index)
    assert pv_update_output.source_name == pv_source_name


@pytest.mark.schema("f144")
def test_update_handler_publishes_enum_update_f144(context, producer, pv_source_name):
    pv_index = 0
    pv_value_str = f"choice{pv_index}"
    pv_timestamp_s = time()  # seconds from unix epoch

    context.call_monitor_callback_with_fake_pv_update(
        NTEnum(valueAlarm=True).wrap(
            {"index": pv_index, "choices": [pv_value_str, "choice1", "choice2"]},
            timestamp=pv_timestamp_s,
        )
    )

    # The assertions below assume that an ep01 message is sent after the f142 and al00
    assert len(producer.published_payloads) == 3
    pv_update_output = deserialise_f144(producer.published_payloads[-3])
    assert np.allclose(pv_update_output.value, pv_index)
    assert pv_update_output.source_name == pv_source_name


@pytest.mark.schema("f142")
@pytest.mark.parametrize(
    "pv_value,pv_type",
    [
        # float
        (4.2222, "d"),
        (4.2, "f"),
        # int
        (1, "l"),
        (2, "L"),
        (-3, "i"),
        (4, "I"),
        (-5, "h"),
        (6, "H"),
        (-7, "b"),
        (8, "B"),
        # floatarray
        (np.array([1.1, 2.2, 3.3], dtype=np.float64), "ad"),
        (np.array([1.1, 2.2, 3.3], dtype=np.float32), "af"),
    ],
)
def test_update_handler_publishes_scalar_update_f142(
    pv_value, pv_type, context, producer, pv_source_name
):
    pv_timestamp_s = time()  # seconds from unix epoch

    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(pv_value, timestamp=pv_timestamp_s)
    )

    # The assertions below assume that an ep01 message is sent after the f142
    assert len(producer.published_payloads) == 2
    pv_update_output = deserialise_f142(producer.published_payloads[-2])
    assert np.allclose(pv_update_output.value, pv_value)
    assert pv_update_output.source_name == pv_source_name


@pytest.mark.schema("f144")
@pytest.mark.parametrize(
    "pv_value,pv_type",
    [
        # float
        (4.2222, "d"),
        (4.2, "f"),
        # int
        (1, "l"),
        (2, "L"),
        (-3, "i"),
        (4, "I"),
        (-5, "h"),
        (6, "H"),
        (-7, "b"),
        (8, "B"),
        # floatarray
        (np.array([1.1, 2.2, 3.3], dtype=np.float64), "ad"),
        (np.array([1.1, 2.2, 3.3], dtype=np.float32), "af"),
    ],
)
def test_update_handler_publishes_scalar_update_f144(
    pv_value, pv_type, context, producer, pv_source_name
):
    pv_timestamp_s = time()  # seconds from unix epoch

    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(pv_value, timestamp=pv_timestamp_s)
    )

    # The assertions below assume that an ep01 message is sent after the f142
    assert len(producer.published_payloads) == 3
    pv_update_output = deserialise_f144(producer.published_payloads[-3])
    assert np.allclose(pv_update_output.value, pv_value)
    assert pv_update_output.source_name == pv_source_name


@pytest.mark.schema("f142")
def test_update_handler_publishes_alarm_update_f142(context, producer, pv_source_name):
    pv_value = 42
    pv_type = "i"
    pv_timestamp_s = time()  # seconds from unix epoch
    alarm_status = 4  # Indicates RECORD alarm, we map the alarm message to a specific alarm status to forward
    alarm_severity = 1  # f142_AlarmSeverity.MINOR
    alarm_message = "HIGH_ALARM"

    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(
            {
                "value": pv_value,
                "alarm": {
                    "status": alarm_status,
                    "severity": alarm_severity,
                    "message": alarm_message,
                },
                "timeStamp": {
                    "secondsPastEpoch": pv_timestamp_s,
                },
            }
        )
    )

    # The assertions below assume that an ep01 message is sent after the f142
    assert len(producer.published_payloads) == 2
    pv_update_output = deserialise_f142(producer.published_payloads[-2])
    assert pv_update_output.source_name == pv_source_name
    assert pv_update_output.alarm_status == f142_AlarmStatus.HIGH  # type: ignore
    assert pv_update_output.alarm_severity == f142_AlarmSeverity.MINOR  # type: ignore


def update_handler_publishes_alarm_update(context, producer, pv_source_name):
    """To be called from schemas that should publish al0x updates. Calling
    tests must be marked with the appropriate schema via 'pytestmark' or
    '@pytest.mark'.
    """
    pv_value = 44
    pv_type = "i"
    pv_timestamp_s = time()  # seconds from unix epoch
    alarm_status = 4  # Indicates RECORD alarm, we map the alarm message to a specific alarm status to forward
    alarm_severity = 1  # al00_Severity.MINOR
    alarm_message = "HIGH_ALARM"

    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(
            {
                "value": pv_value,
                "alarm": {
                    "status": alarm_status,
                    "severity": alarm_severity,
                    "message": alarm_message,
                },
                "timeStamp": {
                    "secondsPastEpoch": pv_timestamp_s,
                },
            }
        )
    )

    al00_messages = [
        msg for msg in producer.published_payloads if "al00" == get_schema(msg)
    ]
    assert len(al00_messages) == 1
    pv_update_output = deserialise_al00(al00_messages[0])
    assert pv_update_output.source == pv_source_name
    assert pv_update_output.severity == al00_Severity.MINOR
    assert pv_update_output.message == alarm_message


@pytest.mark.schema("f144")
def test_update_handler_publishes_alarm_update_f144(context, producer, pv_source_name):
    update_handler_publishes_alarm_update(context, producer, pv_source_name)


@pytest.mark.schema("f144")
def test_update_handler_does_not_republish_identical_alarm_f144(
    context, producer, pv_source_name
):
    pv_value = 44
    pv_type = "i"
    pv_timestamp_s = time()  # seconds from unix epoch
    alarm_status = 4  # Indicates RECORD alarm, we map the alarm message to a specific alarm status to forward
    alarm_severity = 1  # al00_Severity.MINOR
    alarm_message = "HIGH_ALARM"

    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(
            {
                "value": pv_value,
                "alarm": {
                    "status": alarm_status,
                    "severity": alarm_severity,
                    "message": alarm_message,
                },
                "timeStamp": {
                    "secondsPastEpoch": pv_timestamp_s,
                },
            }
        )
    )
    # Second update, with unchanged alarm
    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(
            {
                "value": pv_value,
                "alarm": {
                    "status": alarm_status,
                    "severity": alarm_severity,
                    "message": alarm_message,
                },
                "timeStamp": {
                    "secondsPastEpoch": pv_timestamp_s,
                },
            }
        )
    )

    al00_messages = [
        msg for msg in producer.published_payloads if "al00" == get_schema(msg)
    ]
    assert len(al00_messages) == 1
    pv_update_output = deserialise_al00(al00_messages[0])
    assert pv_update_output.source == pv_source_name
    assert pv_update_output.severity == al00_Severity.MINOR
    assert pv_update_output.message == alarm_message


@pytest.mark.schema("f142")
@pytest.mark.serialiser_update_period_ms(10)
def test_update_handler_publishes_periodic_update_f142(
    context, producer, pv_source_name
):
    pv_value = -3
    pv_type = "i"
    pv_timestamp_s = time()  # seconds from unix epoch

    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(pv_value, timestamp=pv_timestamp_s)
    )

    # The assertions below assume that an ep01 message is sent after the f142
    assert len(producer.published_payloads) == 2
    pv_update_output = deserialise_f142(producer.published_payloads[-2])
    assert np.allclose(pv_update_output.value, pv_value)
    assert pv_update_output.source_name == pv_source_name

    sleep(0.05)
    assert (
        len(producer.published_payloads) >= 3
    ), "Expected more than the 1 message from triggered update due to periodic updates being active"


@pytest.mark.schema("f144")
@pytest.mark.serialiser_update_period_ms(10)
def test_update_handler_publishes_periodic_update_f144(
    context, producer, pv_source_name
):
    pv_value = -3
    pv_type = "i"
    pv_timestamp_s = time()  # seconds from unix epoch

    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(pv_value, timestamp=pv_timestamp_s)
    )

    data_messages = [
        msg for msg in producer.published_payloads if "f144" == get_schema(msg)
    ]
    assert len(data_messages) == 1
    pv_update_output = deserialise_f144(data_messages[0])
    assert pv_update_output.source_name == pv_source_name
    assert np.allclose(pv_update_output.value, pv_value)

    sleep(0.05)
    data_messages = [
        msg for msg in producer.published_payloads if "f144" == get_schema(msg)
    ]
    pv_update_output = [
        deserialise_f144(data_messages[0]),
        deserialise_f144(data_messages[1]),
    ]
    assert (
        len(data_messages) >= 2
    ), "Expected more than the 1 message from triggered update due to periodic updates being active"
    assert (
        pv_update_output[0].timestamp_unix_ns == pv_update_output[1].timestamp_unix_ns
    ), "Expected repeated message timestamps to be equal"


@pytest.mark.schema("tdct")
def test_empty_update_is_not_forwarded(context, producer, pv_source_name):
    pv_timestamp_s = time()  # seconds from unix epoch
    pv_value = [1, 2, 3]
    pv_type = "ai"

    # First update with non-empty value
    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(pv_value, timestamp=pv_timestamp_s)
    )
    # Second update, with empty value
    empty_pv_value: List = []
    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(
            empty_pv_value, timestamp=pv_timestamp_s
        )
    )

    tdct_messages = [
        msg for msg in producer.published_payloads if "tdct" == get_schema(msg)
    ]
    assert (
        len(tdct_messages) == 1
    ), "Expected only the non-empty PV update to have been published"
    pv_update_output = deserialise_tdct(tdct_messages[0])
    assert (
        pv_update_output.timestamps.size > 0
    ), "Expected the published PV update not to be empty"


def test_empty_update_is_not_cached():
    producer = FakeProducer()
    context = FakeContext()

    pv_timestamp_s = time()  # seconds from unix epoch
    pv_source_name = "source_name"
    pv_value: List = []
    pv_type = "ai"

    try:
        pva_update_handler = PVAUpdateHandler(
            context,
            pv_source_name,
            create_serialiser_list(
                producer,  # type: ignore
                pv_source_name,
                "output_topic",
                "tdct",
                EpicsProtocol.PVA,
            ),
        )
        context.call_monitor_callback_with_fake_pv_update(
            NTScalar(pv_type, valueAlarm=True).wrap(pv_value, timestamp=pv_timestamp_s)
        )

        assert (
            pva_update_handler.serialiser_tracker_list[0]._cached_update is None
        ), "Expected the empty update not to have been cached"
    finally:
        pva_update_handler.stop()


@pytest.mark.schema("f144")
@pytest.mark.parametrize(
    "exception,state_enum",
    [
        (Disconnected(), ConnectionInfo.DISCONNECTED),
        (Cancelled(), ConnectionInfo.CANCELLED),
        (Finished(), ConnectionInfo.FINISHED),
        (RemoteError(), ConnectionInfo.REMOTE_ERROR),
        (RuntimeError("some unrecognised exception"), ConnectionInfo.UNKNOWN),
    ],
)
def test_handler_publishes_non_connection_state_when_moving_from_connected(
    exception, state_enum, context, producer, pv_source_name
):
    # Connect first
    context.call_monitor_callback_with_fake_pv_update(
        NTEnum(valueAlarm=True).wrap(
            {"index": 0, "choices": ["choice", "choice1", "choice2"]},
            timestamp=time(),
        )
    )

    context.call_monitor_callback_with_fake_pv_update(exception)

    assert len(producer.published_payloads) > 0
    pv_update_output = deserialise_ep01(producer.published_payloads[-1])
    assert pv_update_output.status == state_enum  # type: ignore
    assert pv_update_output.source_name == pv_source_name  # type: ignore


@pytest.mark.schema("f144")
@pytest.mark.parametrize(
    "exception,state_enum",
    [
        (Disconnected(), ConnectionInfo.DISCONNECTED),
        (Cancelled(), ConnectionInfo.CANCELLED),
        (Finished(), ConnectionInfo.FINISHED),
        (RemoteError(), ConnectionInfo.REMOTE_ERROR),
        (RuntimeError("some unrecognised exception"), ConnectionInfo.UNKNOWN),
    ],
)
def test_handler_does_not_publish_if_never_connected(
    exception, state_enum, context, producer, pv_source_name
):
    context.call_monitor_callback_with_fake_pv_update(exception)

    assert len(producer.published_payloads) == 0
