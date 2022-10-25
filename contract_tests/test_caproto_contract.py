import contextlib
import time

import numpy as np
from caproto.sync.client import write
from caproto.threading.client import Context

# To get both the timestamps and the units for a particular PV
# it is necessary to create two subscriptions to the PV.
# This is because of how CA works.


@contextlib.contextmanager
def read_value(pvname, data_type):
    result = None

    def _callback(sub, response):
        nonlocal result
        if not result:
            result = response

    ctx = Context()
    pv, *_ = ctx.get_pvs(pvname)
    sub = pv.subscribe(data_type=data_type)
    sub.add_callback(_callback)

    start_time = time.monotonic()
    while not result:
        if time.monotonic() > start_time + 2:
            assert False, "timed out for some reason"
        time.sleep(0.1)

    yield result

    sub.clear()


def test_analog_contract_time():
    write("SIMPLE:DOUBLE3", 12, notify=True)

    with read_value("SIMPLE:DOUBLE3", "time") as result:
        assert len(result.data) == 1
        assert result.data[0] == 12

        assert result.metadata.severity == 0
        assert result.metadata.status == 0

        assert isinstance(result.metadata.timestamp, float)


def test_analog_contract_control():
    with read_value("SIMPLE:DOUBLE3", "control") as result:
        assert result.metadata.units == b"K"


def test_alarm_contract():
    # Put into minor
    write("SIMPLE:DOUBLE3", 15, notify=True)

    with read_value("SIMPLE:DOUBLE3", "time") as result:
        assert result.data[0] == 15
        assert result.metadata.severity == 1
        assert result.metadata.status == 4

    # Put into major
    write("SIMPLE:DOUBLE3", 20, notify=True)

    with read_value("SIMPLE:DOUBLE3", "time") as result:
        assert result.data[0] == 20
        assert result.metadata.severity == 2
        assert result.metadata.status == 3

    # Put into no alarm
    write("SIMPLE:DOUBLE3", 13, notify=True)

    with read_value("SIMPLE:DOUBLE3", "time") as result:
        assert result.data[0] == 13
        assert result.metadata.severity == 0
        assert result.metadata.status == 0


def test_string_contract_time():
    write("SIMPLE:STR", "Greetings", notify=True)

    with read_value("SIMPLE:STR", "time") as result:
        assert result.data[0] == b"Greetings"
        assert result.metadata.severity == 0
        assert result.metadata.status == 0

        assert isinstance(result.metadata.timestamp, float)


def test_string_contract_control():
    with read_value("SIMPLE:STR", "control") as result:
        assert not hasattr(result.metadata, "units")


def test_mbbi_contract_time():
    # Set using string
    write("SIMPLE:ENUM", "START", notify=True)

    with read_value("SIMPLE:ENUM", "time") as result:
        assert result.data[0] == 1
        assert result.metadata.severity == 0
        assert result.metadata.status == 0

        assert isinstance(result.metadata.timestamp, float)

    # Set using int
    write("SIMPLE:ENUM", 2, notify=True)
    with read_value("SIMPLE:ENUM", "time") as result:
        assert result.data[0] == 2


def test_mbbi_contract_control():
    with read_value("SIMPLE:ENUM", "control") as result:
        assert result.metadata.enum_strings == (b"INIT", b"START", b"STOP")


def test_binary_contract_time():
    # Set using string
    write("SIMPLE:BOOL", "ONE", notify=True)

    with read_value("SIMPLE:BOOL", "time") as result:
        assert result.data[0] == 1

        assert result.metadata.severity == 0
        assert result.metadata.status == 0

        assert isinstance(result.metadata.timestamp, float)

    # Set using int
    write("SIMPLE:BOOL", 0, notify=True)

    with read_value("SIMPLE:BOOL", "time") as result:
        assert result.data[0] == 0


def test_bi_contract_control():
    with read_value("SIMPLE:BOOL", "control") as result:
        assert result.metadata.enum_strings == (b"ZERO", b"ONE")


def test_numeric_waveform_contract_time():
    write("SIMPLE:LONGARRAY", [9, 8, 7, 6], notify=True)

    with read_value("SIMPLE:LONGARRAY", "time") as result:
        assert np.array_equal(result.data, [9, 8, 7, 6])
        assert result.data.dtype == np.dtype(">f8")

        assert result.metadata.severity == 0
        assert result.metadata.status == 0

        assert isinstance(result.metadata.timestamp, float)


def test_numeric_waveform_contract_control():
    with read_value("SIMPLE:LONGARRAY", "control") as result:
        assert isinstance(result.metadata.units, bytes)


def test_char_waveform_contract_time():
    write("SIMPLE:CHARARRAY", [99, 100, 101], notify=True)

    with read_value("SIMPLE:CHARARRAY", "time") as result:
        assert np.array_equal(result.data, [99, 100, 101])
        assert result.data.dtype == np.dtype("uint8")

        assert result.metadata.severity == 0
        assert result.metadata.status == 0

        assert isinstance(result.metadata.timestamp, float)


def test_char_waveform_contract_control():
    with read_value("SIMPLE:CHARARRAY", "control") as result:
        assert isinstance(result.metadata.units, bytes)


def test_string_waveform_contract_time():
    write("SIMPLE:STRINGARRAY", ["Hi", "Bye"], notify=True)

    with read_value("SIMPLE:STRINGARRAY", "time") as result:
        assert np.array_equal(result.data, [b"Hi", b"Bye"])

        assert result.metadata.severity == 0
        assert result.metadata.status == 0

        assert isinstance(result.metadata.timestamp, float)


def test_string_waveform_contract_control():
    with read_value("SIMPLE:STRINGARRAY", "control") as result:
        assert not hasattr(result.metadata, "units")


def test_analog_array_contract_time():
    write("SIMPLE:AAI", [1, 5, 10, 3, 0], notify=True)

    with read_value("SIMPLE:AAI", "time") as result:
        assert np.array_equal(result.data, [1, 5, 10, 3, 0])
        assert result.data.dtype == np.dtype(">f8")

        assert result.metadata.severity == 0
        assert result.metadata.status == 0

        assert isinstance(result.metadata.timestamp, float)


def test_analog_array_contract_control():
    with read_value("SIMPLE:AAI", "control") as result:
        assert isinstance(result.metadata.units, bytes)


def test_connection_callback():
    result = None

    def _conn_callback(sub, response):
        nonlocal result
        if not result:
            result = response

    ctx = Context()
    pv, *_ = ctx.get_pvs("SIMPLE:DOUBLE3", connection_state_callback=_conn_callback)
    sub = pv.subscribe(data_type="control")

    while not result:
        time.sleep(0.1)

    assert result == "connected"

    sub.clear()


def test_value_callback_when_units_change():
    result = None

    def _callback(sub, response):
        nonlocal result
        if not result:
            result = response

    ctx = Context()
    pv, *_ = ctx.get_pvs("SIMPLE:DOUBLE")
    sub = pv.subscribe(data_type="control")
    sub.add_callback(_callback)

    start_time = time.monotonic()
    while not result:
        if time.monotonic() > start_time + 2:
            assert False, "timed out for some reason"
        time.sleep(0.1)

    old_units = result.metadata.units
    result = None

    write("SIMPLE:DOUBLE.EGU", "X" if old_units != b"X" else "Y", notify=True)

    start_time = time.monotonic()
    while not result:
        if time.monotonic() > start_time + 2:
            assert False, "timed out for some reason"
        time.sleep(0.1)

    assert old_units != result.metadata.units
