import contextlib
import numbers
import time

import numpy as np
from p4p.client.thread import Context

TIMEOUT = 5


@contextlib.contextmanager
def read_value(pvname):
    result = None

    def _callback(response):
        nonlocal result
        if isinstance(response, Exception):
            return
        if not result:
            result = response

    ctx = Context("pva", nt=False)
    request = ctx.makeRequest("field(value,timeStamp,alarm,control,display)")
    subscription = ctx.monitor(
        pvname, _callback, request=request, notify_disconnect=True
    )

    start_time = time.monotonic()
    while not result:
        if time.monotonic() > start_time + 5:
            assert False, "timed out for some reason"
        time.sleep(0.1)

    yield result

    subscription.close()
    ctx.close()


def test_analog_contract():
    ctx = Context("pva", nt=False)
    ctx.put("SIMPLE:DOUBLE3", 12, wait=True)

    with read_value("SIMPLE:DOUBLE3") as result:
        assert result.getID() == "epics:nt/NTScalar:1.0"
        assert result.type()["value"] == "d"  # double

        assert isinstance(result["value"], numbers.Number)
        assert result["value"] == 12

        assert set(result["alarm"].keys()) == {"severity", "status", "message"}
        assert result["alarm"]["severity"] == 0
        assert result["alarm"]["status"] == 0
        assert result["alarm"]["message"] == "NO_ALARM"

        assert "timeStamp" in result
        assert set(result["timeStamp"].keys()) == {
            "secondsPastEpoch",
            "nanoseconds",
            "userTag",
        }
        assert isinstance(result["timeStamp"]["secondsPastEpoch"], numbers.Number)
        assert isinstance(result["timeStamp"]["nanoseconds"], numbers.Number)

        assert "display" in result
        assert "units" in result["display"]
        assert result["display"]["units"] == "K"


def test_alarm_contract():
    ctx = Context("pva", nt=False)

    # Put into minor
    ctx.put("SIMPLE:DOUBLE3", 15, wait=True)

    with read_value("SIMPLE:DOUBLE3") as result:
        assert result["value"] == 15
        assert result["alarm"]["severity"] == 1
        assert result["alarm"]["status"] == 1
        assert result["alarm"]["message"] == "HIGH"

    # Put into major
    ctx.put("SIMPLE:DOUBLE3", 20, wait=True)

    with read_value("SIMPLE:DOUBLE3") as result:
        assert result["value"] == 20
        assert result["alarm"]["severity"] == 2
        assert result["alarm"]["status"] == 1
        assert result["alarm"]["message"] == "HIHI"

    # Put into no alarm
    ctx.put("SIMPLE:DOUBLE3", 13, wait=True)

    with read_value("SIMPLE:DOUBLE3") as result:
        assert result["value"] == 13
        assert result["alarm"]["severity"] == 0
        assert result["alarm"]["status"] == 0
        assert result["alarm"]["message"] == "NO_ALARM"


def test_string_contract():
    ctx = Context("pva", nt=False)
    ctx.put("SIMPLE:STR", "HELLO", wait=True)

    with read_value("SIMPLE:STR") as result:
        assert result.getID() == "epics:nt/NTScalar:1.0"
        assert result.type()["value"] == "s"  # string

        assert result["value"] == "HELLO"

        assert set(result["alarm"].keys()) == {"severity", "status", "message"}
        assert result["alarm"]["severity"] == 0
        assert result["alarm"]["status"] == 0
        assert result["alarm"]["message"] == "NO_ALARM"

        assert "timeStamp" in result
        assert set(result["timeStamp"].keys()) == {
            "secondsPastEpoch",
            "nanoseconds",
            "userTag",
        }
        assert isinstance(result["timeStamp"]["secondsPastEpoch"], numbers.Number)
        assert isinstance(result["timeStamp"]["nanoseconds"], numbers.Number)

        assert "display" in result
        assert "units" in result["display"]
        assert isinstance(result["display"]["units"], str)


def test_mbbi_contract():
    ctx = Context("pva", nt=False)
    # Set using string
    ctx.put("SIMPLE:ENUM", "START", wait=True)

    with read_value("SIMPLE:ENUM") as result:
        assert result.getID() == "epics:nt/NTEnum:1.0"

        assert set(result["value"].keys()) == {"index", "choices"}
        assert result["value"]["index"] == 1
        assert result["value"]["choices"] == ["INIT", "START", "STOP"]

        assert "alarm" in result
        assert set(result["alarm"].keys()) == {"severity", "status", "message"}
        assert result["alarm"]["severity"] == 0
        assert result["alarm"]["status"] == 0
        assert result["alarm"]["message"] == "NO_ALARM"

        assert "timeStamp" in result
        assert set(result["timeStamp"].keys()) == {
            "secondsPastEpoch",
            "nanoseconds",
            "userTag",
        }
        assert isinstance(result["timeStamp"]["secondsPastEpoch"], numbers.Number)
        assert isinstance(result["timeStamp"]["nanoseconds"], numbers.Number)

        assert "display" not in result

    # Set using int
    ctx.put("SIMPLE:ENUM", 2, wait=True)

    with read_value("SIMPLE:ENUM") as result:
        assert result["value"]["index"] == 2


def test_binary_contract():
    ctx = Context("pva", nt=False)
    # Set using string
    ctx.put("SIMPLE:BOOL", "ONE", wait=True)

    with read_value("SIMPLE:BOOL") as result:
        assert result.getID() == "epics:nt/NTEnum:1.0"

        assert set(result["value"].keys()) == {"index", "choices"}
        assert result["value"]["index"] == 1
        assert result["value"]["choices"] == ["ZERO", "ONE"]

        assert "alarm" in result
        assert set(result["alarm"].keys()) == {"severity", "status", "message"}
        assert result["alarm"]["severity"] == 0
        assert result["alarm"]["status"] == 0
        assert result["alarm"]["message"] == "NO_ALARM"

        assert "timeStamp" in result
        assert set(result["timeStamp"].keys()) == {
            "secondsPastEpoch",
            "nanoseconds",
            "userTag",
        }
        assert isinstance(result["timeStamp"]["secondsPastEpoch"], numbers.Number)
        assert isinstance(result["timeStamp"]["nanoseconds"], numbers.Number)

        assert "display" not in result

    # Set using string
    ctx.put("SIMPLE:BOOL", 0, wait=True)

    with read_value("SIMPLE:BOOL") as result:
        assert result["value"]["index"] == 0


def test_numeric_waveform_contract():
    ctx = Context("pva", nt=False)
    ctx.put("SIMPLE:LONGARRAY", [1, 2, 3, 4], wait=True)

    with read_value("SIMPLE:LONGARRAY") as result:
        assert result.getID() == "epics:nt/NTScalarArray:1.0"
        assert result.type()["value"] == "al"  # array of longs

        assert isinstance(result["value"], np.ndarray)
        assert result["value"].dtype == np.int64
        assert np.array_equal(result["value"], [1, 2, 3, 4])

        assert set(result["alarm"].keys()) == {"severity", "status", "message"}
        assert result["alarm"]["severity"] == 0
        assert result["alarm"]["status"] == 0
        assert result["alarm"]["message"] == "NO_ALARM"

        assert "timeStamp" in result
        assert set(result["timeStamp"].keys()) == {
            "secondsPastEpoch",
            "nanoseconds",
            "userTag",
        }
        assert isinstance(result["timeStamp"]["secondsPastEpoch"], numbers.Number)
        assert isinstance(result["timeStamp"]["nanoseconds"], numbers.Number)

        assert "display" in result
        assert "units" in result["display"]
        assert isinstance(result["display"]["units"], str)


def test_char_waveform_contract():
    ctx = Context("pva", nt=False)
    ctx.put("SIMPLE:CHARARRAY", [97, 98, 99], wait=True)

    with read_value("SIMPLE:CHARARRAY") as result:
        assert result.getID() == "epics:nt/NTScalarArray:1.0"
        assert result.type()["value"] == "ab"  # array of bytes

        assert "value" in result
        assert isinstance(result["value"], np.ndarray)
        assert np.array_equal(result["value"], [97, 98, 99])

        assert set(result["alarm"].keys()) == {"severity", "status", "message"}
        assert result["alarm"]["severity"] == 0
        assert result["alarm"]["status"] == 0
        assert result["alarm"]["message"] == "NO_ALARM"

        assert "timeStamp" in result
        assert set(result["timeStamp"].keys()) == {
            "secondsPastEpoch",
            "nanoseconds",
            "userTag",
        }
        assert isinstance(result["timeStamp"]["secondsPastEpoch"], numbers.Number)
        assert isinstance(result["timeStamp"]["nanoseconds"], numbers.Number)

        assert "display" in result
        assert "units" in result["display"]
        assert isinstance(result["display"]["units"], str)


def test_str_waveform_contract():
    ctx = Context("pva", nt=False)
    ctx.put("SIMPLE:STRINGARRAY", ["hello", "goodbye"], wait=True)

    with read_value("SIMPLE:STRINGARRAY") as result:
        assert result.getID() == "epics:nt/NTScalarArray:1.0"
        assert result.type()["value"] == "as"  # array of strings

        assert isinstance(result["value"], list)
        assert np.array_equal(result["value"], ["hello", "goodbye"])

        assert set(result["alarm"].keys()) == {"severity", "status", "message"}
        assert result["alarm"]["severity"] == 0
        assert result["alarm"]["status"] == 0
        assert result["alarm"]["message"] == "NO_ALARM"

        assert "timeStamp" in result
        assert set(result["timeStamp"].keys()) == {
            "secondsPastEpoch",
            "nanoseconds",
            "userTag",
        }
        assert isinstance(result["timeStamp"]["secondsPastEpoch"], numbers.Number)
        assert isinstance(result["timeStamp"]["nanoseconds"], numbers.Number)

        assert "display" in result
        assert "units" in result["display"]
        assert isinstance(result["display"]["units"], str)


def test_analog_array_contract():
    ctx = Context("pva", nt=False)
    ctx.put("SIMPLE:AAI", [1, 5, 10, 3, 0], wait=True)

    with read_value("SIMPLE:AAI") as result:
        assert result.getID() == "epics:nt/NTScalarArray:1.0"

        assert "value" in result
        assert isinstance(result["value"], np.ndarray)
        assert result.type()["value"] == "al"  # array of longs
        assert np.array_equal(result["value"], [1, 5, 10, 3, 0])

        assert set(result["alarm"].keys()) == {"severity", "status", "message"}
        assert result["alarm"]["severity"] == 0
        assert result["alarm"]["status"] == 0
        assert result["alarm"]["message"] == "NO_ALARM"

        assert "timeStamp" in result
        assert set(result["timeStamp"].keys()) == {
            "secondsPastEpoch",
            "nanoseconds",
            "userTag",
        }
        assert isinstance(result["timeStamp"]["secondsPastEpoch"], numbers.Number)
        assert isinstance(result["timeStamp"]["nanoseconds"], numbers.Number)

        assert "display" in result
        assert "units" in result["display"]
        assert isinstance(result["display"]["units"], str)


def test_monitor_disconnects_raises():
    raises_exception = False

    def _callback(response):
        nonlocal raises_exception
        if isinstance(response, Exception):
            raises_exception = True

    ctx = Context("pva", nt=False)
    request = ctx.makeRequest("field(value,timeStamp,alarm,control,display)")
    subscription = ctx.monitor(
        "SIMPLE:UNAVAILABLE", _callback, request=request, notify_disconnect=True
    )

    start_time = time.monotonic()
    while not raises_exception:
        if time.monotonic() > start_time + TIMEOUT:
            assert False, "timed out for some reason"

    assert raises_exception

    subscription.close()
    ctx.close()
