from datetime import datetime, timezone
from time import time

import numpy as np
import pytest
from numpy.typing import NDArray
from p4p import Value
from p4p.nt import NTTable
from streaming_data_types.alarm_al00 import Severity as al00_Severity
from streaming_data_types.alarm_al00 import deserialise_al00
from streaming_data_types.sample_environment_senv import deserialise_senv
from streaming_data_types.utils import get_schema

from forwarder.common import EpicsProtocol
from forwarder.update_handlers.nttable_senv_serialiser import nttable_senv_PVASerialiser

# Set marks required by the 'context' fixture.
pytestmark = [
    pytest.mark.epics_protocol(EpicsProtocol.PVA),
    pytest.mark.schema("nttable_senv"),
]


def test_serialise_nttable():
    values: NDArray = np.arange(-50, 50, dtype=np.int16)
    timestamps: NDArray = np.arange(50, 150, dtype=np.uint64)
    first_timestamp = timestamps[0] / 1e9

    table = NTTable.buildType(
        columns=[
            ("column0", "ah"),
            ("column1", "aL"),
        ]
    )

    update = Value(
        table,
        {
            "labels": ["value", "timestamp"],
            "value": {"column0": values, "column1": timestamps},
        },
    )

    pv_name = "some_pv"
    serialiser = nttable_senv_PVASerialiser(pv_name)
    message, timestamp = serialiser.serialise(update)

    fb_update = deserialise_senv(message)

    assert fb_update.name == pv_name
    assert np.array_equal(fb_update.values, values)
    assert fb_update.values.dtype == values.dtype
    assert np.array_equal(fb_update.value_ts, timestamps)
    assert fb_update.timestamp == datetime.fromtimestamp(
        float(first_timestamp), tz=timezone.utc
    )
    assert fb_update.message_counter == 0


def test_update_handler_publishes_senv_alarm_update(context, producer, pv_source_name):
    pv_timestamp_s = time()  # seconds from unix epoch
    values: NDArray = np.arange(-50, 50, 11, dtype=np.float32)
    timestamps = np.repeat(int(time()), 100)
    alarm_status = 4  # Indicates RECORD alarm, we map the alarm message to a specific alarm status to forward
    alarm_severity = 1  # al00_Severity.MINOR
    alarm_message = "HIGH_ALARM"
    table = NTTable.buildType(
        columns=[
            ("column0", "af"),
            ("column1", "al"),
        ],
    )
    update = Value(
        table,
        {
            "labels": ["value", "timestamp"],
            "value": {"column0": values, "column1": timestamps},
            "alarm": {
                "status": alarm_status,
                "severity": alarm_severity,
                "message": alarm_message,
            },
            "timeStamp": {
                "secondsPastEpoch": pv_timestamp_s,
            },
        },
    )

    context.call_monitor_callback_with_fake_pv_update(update)

    al00_messages = [
        msg for msg in producer.published_payloads if "al00" == get_schema(msg)
    ]
    assert len(al00_messages) == 1
    pv_update_output = deserialise_al00(al00_messages[0])
    assert pv_update_output.source == pv_source_name
    assert pv_update_output.severity == al00_Severity.MINOR
    assert pv_update_output.message == alarm_message
