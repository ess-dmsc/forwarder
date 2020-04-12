from helpers.f142_logdata import Int, Double, String, ArrayFloat, ArrayDouble, LogData
from helpers.f142_logdata.Value import Value
from helpers.f142_logdata.AlarmSeverity import AlarmSeverity
from helpers.f142_logdata.AlarmStatus import AlarmStatus
from cmath import isclose
import numpy as np

ValueTypes = {
    Value.Int: Int.Int,
    Value.Double: Double.Double,
    Value.String: String.String,
    Value.ArrayFloat: ArrayFloat.ArrayFloat,
    Value.ArrayDouble: ArrayDouble.ArrayDouble,
}


def check_multiple_expected_values(message_list, expected_values):
    """
    Checks for expected PV values in multiple messages.
    Note: not order/time-specific, and requires PVs to have different names.

    :param message_list: A list of flatbuffers objects
    :param expected_values:  A dict with PV names as keys for expected value types and values
    :return: None
    """
    used_pv_names = []
    for log_data in message_list:
        name = str(log_data.SourceName(), encoding="utf-8")
        assert name in expected_values.keys() and name not in used_pv_names
        used_pv_names.append(name)
        check_expected_value(
            log_data, expected_values[name][0], name, expected_values[name][1]
        )


def check_expected_value(log_data: LogData, value_type, pv_name, expected_value=None):
    """
    Checks the message name (PV) and value type (type of PV), and, optionally, the value.

    :param log_data: Log data object from the received stream buffer
    :param value_type: Flatbuffers value type
    :param pv_name: Byte encoded string of the PV/channel name
    :param expected_value: The expected PV value from the flatbuffers message, can be a list or tuple for arrays
    :return: None
    """

    assert value_type == log_data.ValueType()
    assert bytes(pv_name, encoding="utf-8") == log_data.SourceName()
    assert log_data.Timestamp() > 0

    def is_sequence(obj):
        return type(obj) in [list, tuple]

    if expected_value is not None:
        union_val = ValueTypes[value_type]()
        union_val.Init(log_data.Value().Bytes, log_data.Value().Pos)

        if is_sequence(expected_value):
            value_array = union_val.ValueAsNumpy()
            assert np.allclose(value_array, np.array(expected_value))
        else:
            print(
                f"expected value: {expected_value}, value from message: {union_val.Value()}",
                flush=True,
            )
            if isinstance(expected_value, float):
                assert isclose(expected_value, union_val.Value())
            else:
                assert expected_value == union_val.Value()


def check_expected_alarm_status(
    log_data: LogData, expected_status: AlarmStatus, expected_severity: AlarmSeverity
):
    assert (
        log_data.Severity() == expected_severity
    ), f"Actual alarm severity: {log_data.Severity()}, Expected alarm severity: {expected_severity}"
    assert (
        log_data.Status() == expected_status
    ), f"Actual alarm status: {log_data.Status()}, Expected alarm status: {expected_status}"
