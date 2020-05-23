from cmath import isclose
import numpy as np
from streaming_data_types.logdata_f142 import deserialise_f142
from typing import Any
from helpers.f142_logdata.AlarmSeverity import AlarmSeverity
from helpers.f142_logdata.AlarmStatus import AlarmStatus


def check_multiple_expected_values(message_list, expected_values):
    """
    Checks for expected PV values in multiple messages.
    Note: not order/time-specific, and requires PVs to have different names.

    :param message_list: A list of flatbuffers objects
    :param expected_values:  A dict with PV names as keys for expected value types and values
    """
    used_pv_names = []
    for log_data_buffer in message_list:
        log_data = deserialise_f142(log_data_buffer)
        assert (
            log_data.source_name in expected_values.keys()
            and log_data.source_name not in used_pv_names
        )
        used_pv_names.append(log_data.source_name)
        check_expected_value(
            log_data_buffer, log_data.source_name, expected_values[log_data.source_name]
        )


def check_expected_value(
    log_data_buffer: bytes, pv_name: str, expected_value: Any = None
):
    """
    Checks the message name (PV) and value type (type of PV), and, optionally, the value.

    :param log_data_buffer: Received message payload
    :param pv_name: PV/channel name
    :param expected_value: The expected PV value from the message, can be a list or tuple for arrays
    """
    log_data = deserialise_f142(log_data_buffer)

    assert log_data.source_name == pv_name
    assert log_data.timestamp_unix_ns > 0

    def is_sequence(obj):
        return type(obj) in [list, tuple]

    if expected_value is not None:
        if is_sequence(expected_value):
            assert np.allclose(log_data.value, np.array(expected_value))
        else:
            if isinstance(expected_value, float):
                assert isclose(log_data.value, expected_value)
            else:
                assert (
                    log_data.value == expected_value
                ), f"Expected {expected_value}, got {log_data.value}"


def check_expected_alarm_status(
    log_data_buffer: bytes,
    expected_status: AlarmStatus,
    expected_severity: AlarmSeverity,
):
    log_data = deserialise_f142(log_data_buffer)
    assert (
        log_data.alarm_severity == expected_severity
    ), f"Actual alarm severity: {log_data.alarm_severity}, Expected alarm severity: {expected_severity}"
    assert (
        log_data.alarm_status == expected_status
    ), f"Actual alarm status: {log_data.alarm_status}, Expected alarm status: {expected_status}"
