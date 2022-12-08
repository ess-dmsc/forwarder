from cmath import isclose
from time import sleep, time
from typing import List

import numpy as np
import pytest
from p4p.client.thread import Cancelled, Disconnected, Finished, RemoteError
from p4p.nt import NTEnum, NTScalar
from streaming_data_types.epics_connection_ep01 import ConnectionInfo, deserialise_ep01
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import AlarmSeverity
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
from streaming_data_types.logdata_f142 import deserialise_f142
from streaming_data_types.timestamps_tdct import deserialise_tdct

from forwarder.common import EpicsProtocol
from forwarder.update_handlers.pva_update_handler import PVAUpdateHandler
from forwarder.update_handlers.serialiser_tracker import create_serialiser_list
from tests.kafka.fake_producer import FakeProducer
from tests.test_helpers.p4p_fakes import FakeContext


def test_update_handler_throws_if_schema_not_recognised():
    producer = FakeProducer()
    context = FakeContext()
    non_existing_schema = "DOESNTEXIST"
    with pytest.raises(ValueError):
        PVAUpdateHandler(context, "source_name", create_serialiser_list(producer, "source_name", "output_topic", non_existing_schema, EpicsProtocol.PVA))  # type: ignore


def test_update_handler_publishes_enum_update():
    context = FakeContext()

    source_name = ""
    got_value = None

    def check_payload(payload):
        nonlocal source_name, got_value
        try:
            result = deserialise_f142(payload)
            source_name = result.source_name
            got_value = result.value
        except Exception:
            pass

    producer = FakeProducer(check_payload)

    pv_index = 0
    pv_value_str = "choice0"
    pv_timestamp_s = time()  # seconds from unix epoch
    pv_source_name = "source_name"

    pva_update_handler = PVAUpdateHandler(context, pv_source_name, create_serialiser_list(producer, pv_source_name, "output_topic", "f142", EpicsProtocol.PVA))  # type: ignore
    context.call_monitor_callback_with_fake_pv_update(
        NTEnum(valueAlarm=True).wrap(
            {"index": pv_index, "choices": [pv_value_str, "choice1", "choice2"]},
            timestamp=pv_timestamp_s,
        )
    )

    assert got_value == 0
    assert source_name == pv_source_name

    pva_update_handler.stop()


@pytest.mark.parametrize("pv_value,pv_type", [(4.2222, "d"), (4.2, "f")])
def test_update_handler_publishes_float_update(pv_value, pv_type):
    context = FakeContext()

    source_name = ""
    got_value = 0.0

    def check_payload(payload):
        nonlocal source_name, got_value
        try:
            result = deserialise_f142(payload)
            source_name = result.source_name
            got_value = result.value
        except Exception:
            pass

    producer = FakeProducer(check_payload)

    pv_timestamp_s = time()  # seconds from unix epoch
    pv_source_name = "source_name"

    pva_update_handler = PVAUpdateHandler(context, pv_source_name, create_serialiser_list(producer, pv_source_name, "output_topic", "f142", EpicsProtocol.PVA))  # type: ignore
    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(pv_value, timestamp=pv_timestamp_s)
    )

    assert isclose(got_value, pv_value, abs_tol=0.0001)
    assert source_name == pv_source_name

    pva_update_handler.stop()


@pytest.mark.parametrize(
    "pv_value,pv_type",
    [(1, "l"), (2, "L"), (-3, "i"), (4, "I"), (-5, "h"), (6, "H"), (-7, "b"), (8, "B")],
)
def test_update_handler_publishes_int_update(pv_value, pv_type):
    context = FakeContext()

    source_name = ""
    got_value = None

    def check_payload(payload):
        nonlocal source_name, got_value
        try:
            result = deserialise_f142(payload)
            source_name = result.source_name
            got_value = result.value
        except Exception:
            pass

    producer = FakeProducer(check_payload)

    pv_timestamp_s = time()  # seconds from unix epoch
    pv_source_name = "source_name"

    pva_update_handler = PVAUpdateHandler(context, pv_source_name, create_serialiser_list(producer, pv_source_name, "output_topic", "f142", EpicsProtocol.PVA))  # type: ignore
    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(pv_value, timestamp=pv_timestamp_s)
    )

    assert got_value == pv_value
    assert source_name == pv_source_name

    pva_update_handler.stop()


@pytest.mark.parametrize(
    "pv_value,pv_type",
    [
        (np.array([1.1, 2.2, 3.3], dtype=np.float64), "ad"),
        (np.array([1.1, 2.2, 3.3], dtype=np.float32), "af"),
    ],
)
def test_update_handler_publishes_floatarray_update(pv_value, pv_type):
    context = FakeContext()

    source_name = ""
    got_value = None

    def check_payload(payload):
        nonlocal source_name, got_value
        try:
            result = deserialise_f142(payload)
            source_name = result.source_name
            got_value = result.value
        except Exception:
            pass

    producer = FakeProducer(check_payload)

    pv_timestamp_s = time()  # seconds from unix epoch
    pv_source_name = "source_name"

    pva_update_handler = PVAUpdateHandler(context, pv_source_name, create_serialiser_list(producer, pv_source_name, "output_topic", "f142", EpicsProtocol.PVA))  # type: ignore
    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(pv_value, timestamp=pv_timestamp_s)
    )

    assert np.allclose(got_value, pv_value)
    assert source_name == pv_source_name

    pva_update_handler.stop()


def test_update_handler_publishes_alarm_update():
    context = FakeContext()

    result = None

    def check_payload(payload):
        nonlocal result
        try:
            result = deserialise_f142(payload)
        except Exception:
            pass

    producer = FakeProducer(check_payload)

    pv_value = 42
    pv_type = "i"
    pv_timestamp_s = time()  # seconds from unix epoch
    pv_source_name = "source_name"
    alarm_status = 4  # Indicates RECORD alarm, we map the alarm message to a specific alarm status to forward
    alarm_severity = 1  # AlarmSeverity.MINOR
    alarm_message = "HIGH_ALARM"

    pva_update_handler = PVAUpdateHandler(context, pv_source_name, create_serialiser_list(producer, pv_source_name, "output_topic", "f142", EpicsProtocol.PVA))  # type: ignore
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

    assert result.value == pv_value  # type: ignore
    assert result.source_name == pv_source_name  # type: ignore
    assert result.alarm_status == AlarmStatus.HIGH  # type: ignore
    assert result.alarm_severity == AlarmSeverity.MINOR  # type: ignore

    pva_update_handler.stop()


def test_update_handler_publishes_periodic_update():
    result = None

    def check_payload(payload):
        nonlocal result
        try:
            result = deserialise_f142(payload)
        except Exception:
            pass

    producer = FakeProducer(check_payload)
    context = FakeContext()

    pv_timestamp_s = time()  # seconds from unix epoch
    pv_source_name = "source_name"
    pv_value = -3
    pv_type = "i"

    update_period_ms = 10
    pva_update_handler = PVAUpdateHandler(context, pv_source_name, create_serialiser_list(producer, pv_source_name, "output_topic", "f142", EpicsProtocol.PVA, update_period_ms))  # type: ignore
    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(pv_value, timestamp=pv_timestamp_s)
    )

    assert result.value == pv_value  # type: ignore
    assert result.source_name == pv_source_name  # type: ignore

    sleep(0.05)
    assert (
        producer.messages_published > 1
    ), "Expected more than the 1 message from triggered update due to periodic updates being active"

    pva_update_handler.stop()


def test_empty_update_is_not_forwarded():
    result = None

    def check_payload(payload):
        nonlocal result
        try:
            result = deserialise_tdct(payload)
        except Exception:
            pass

    producer = FakeProducer(check_payload)
    context = FakeContext()

    pv_timestamp_s = time()  # seconds from unix epoch
    pv_source_name = "source_name"
    pv_value = [1, 2, 3]
    pv_type = "ai"

    pva_update_handler = PVAUpdateHandler(context, pv_source_name, create_serialiser_list(producer, pv_source_name, "output_topic", "tdct", EpicsProtocol.PVA))  # type: ignore

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

    assert (
        producer.messages_published == 2
    ), "Expected only two PV updates with non-empty value array to have been published (tdct + ep01)"
    assert (
        result.timestamps.size > 0  # type: ignore
    ), "Expected the published PV update not to be empty"

    pva_update_handler.stop()


def test_empty_update_is_not_cached():
    producer = FakeProducer()
    context = FakeContext()

    pv_timestamp_s = time()  # seconds from unix epoch
    pv_source_name = "source_name"
    pv_value: List = []
    pv_type = "ai"

    pva_update_handler = PVAUpdateHandler(context, pv_source_name, create_serialiser_list(producer, pv_source_name, "output_topic", "tdct", EpicsProtocol.PVA))  # type: ignore
    context.call_monitor_callback_with_fake_pv_update(
        NTScalar(pv_type, valueAlarm=True).wrap(pv_value, timestamp=pv_timestamp_s)
    )

    assert (
        pva_update_handler.serialiser_tracker_list[0]._cached_update is None
    ), "Expected the empty update not to have been cached"

    pva_update_handler.stop()


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
def test_handler_publishes_connection_state_change(exception, state_enum):
    result = None

    def check_payload(payload):
        nonlocal result
        try:
            result = deserialise_ep01(payload)
        except Exception:
            pass

    producer = FakeProducer(check_payload)
    context = FakeContext()

    pv_source_name = "source_name"

    pva_update_handler = PVAUpdateHandler(context, pv_source_name, create_serialiser_list(producer, pv_source_name, "output_topic", "f142", EpicsProtocol.PVA))  # type: ignore
    context.call_monitor_callback_with_fake_pv_update(exception)

    assert len(producer.published_payloads) > 0
    assert result.status == state_enum  # type: ignore
    assert result.source_name == pv_source_name  # type: ignore

    pva_update_handler.stop()


def test_connection_state_change_on_cancel():
    result = None

    def check_payload(payload):
        nonlocal result
        try:
            result = deserialise_ep01(payload)
        except Exception:
            pass

    producer = FakeProducer(check_payload)
    context = FakeContext()

    pv_source_name = "source_name"

    pva_update_handler = PVAUpdateHandler(context, pv_source_name, create_serialiser_list(producer, pv_source_name, "output_topic", "f142", EpicsProtocol.PVA))  # type: ignore
    context.call_monitor_callback_with_fake_pv_update(Cancelled())
    # "Cancelled" occurs when we intentionally disconnect the client,
    # we don't log this to Kafka as a connection state change

    assert len(producer.published_payloads) > 0

    pva_update_handler.stop()
