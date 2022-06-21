from p4p.client.thread import Disconnected
from streaming_data_types.epics_pv_alarm_status_pvAl import (
    deserialise_pvAl,
    AlarmSeverity,
    CAAlarmState,
    AlarmState,
)

from forwarder.update_handlers.pvAl_serialiser import pvAl_Serialiser
from p4p.nt import NTScalar
from datetime import datetime, timedelta, timezone
from caproto import ReadNotifyResponse, TimeStamp, timestamp_to_epics, ChannelType


def test_serialise_pva_value():
    reference_time = datetime.now(tz=timezone.utc)
    update = NTScalar("i").wrap(3.134)
    update.timeStamp.secondsPastEpoch = int(reference_time.timestamp())
    update.timeStamp.nanoseconds = int(
        (reference_time.timestamp() - int(reference_time.timestamp())) * 1e9
    )
    update.alarm.status = 1
    update.alarm.severity = 3
    update.alarm.message = "Some alarm message"

    pv_name = "some_pv"
    serialiser = pvAl_Serialiser(pv_name)
    message, timestamp = serialiser.pva_serialise(update)

    fb_update = deserialise_pvAl(message)

    assert fb_update.source_name == pv_name
    assert fb_update.timestamp == reference_time
    assert fb_update.state == AlarmState.AlarmState.DEVICE
    assert fb_update.severity == AlarmSeverity.AlarmSeverity.INVALID
    assert fb_update.message == update.alarm.message
    assert fb_update.ca_state == CAAlarmState.CAAlarmState.UDF


def test_serialise_pva_disconnected():
    pv_name = "some_pv"
    serialiser = pvAl_Serialiser(pv_name)
    message, timestamp = serialiser.pva_serialise(Disconnected())

    fb_update = deserialise_pvAl(message)

    assert fb_update.source_name == pv_name
    assert datetime.now(tz=timezone.utc) - fb_update.timestamp < timedelta(seconds=1)
    assert fb_update.state == AlarmState.AlarmState.UNDEFINED
    assert fb_update.ca_state == CAAlarmState.CAAlarmState.UDF
    assert fb_update.severity == AlarmSeverity.AlarmSeverity.UNDEFINED


def test_serialise_pva_unknown_alarm():
    update = NTScalar("i").wrap(3.134)
    update.alarm.status = 543

    serialiser = pvAl_Serialiser("pv_name")
    message, timestamp = serialiser.pva_serialise(update)

    fb_update = deserialise_pvAl(message)

    assert fb_update.state == AlarmState.AlarmState.UNDEFINED
    assert fb_update.severity == AlarmSeverity.AlarmSeverity.UNDEFINED


def test_serialise_ca_alarm():
    pv_name = "some_pv"
    reference_time = datetime.now(tz=timezone.utc)
    serialiser = pvAl_Serialiser(pv_name)

    metadata = (
        CAAlarmState.CAAlarmState.DISABLE.real,
        AlarmSeverity.AlarmSeverity.MAJOR.real,
        TimeStamp(*timestamp_to_epics(reference_time.replace(tzinfo=None))),
    )

    update = ReadNotifyResponse(
        [
            3.14,
        ],
        ChannelType.TIME_DOUBLE,
        1,
        1,
        1,
        metadata=metadata,
    )

    message, timestamp = serialiser.ca_serialise(update)

    fb_update = deserialise_pvAl(message)

    assert fb_update.source_name == pv_name
    assert fb_update.timestamp == reference_time
    assert fb_update.state == AlarmState.AlarmState.DB
    assert fb_update.severity == AlarmSeverity.AlarmSeverity.MAJOR
    assert fb_update.message == ""
    assert fb_update.ca_state == CAAlarmState.CAAlarmState.DISABLE


def test_serialise_ca_alarm_undefined():
    pv_name = "some_pv"
    reference_time = datetime.now(tz=timezone.utc)
    serialiser = pvAl_Serialiser(pv_name)

    metadata = (
        453,
        0,
        TimeStamp(*timestamp_to_epics(reference_time.replace(tzinfo=None))),
    )

    update = ReadNotifyResponse(
        [
            3.14,
        ],
        ChannelType.TIME_DOUBLE,
        1,
        1,
        1,
        metadata=metadata,
    )

    message, timestamp = serialiser.ca_serialise(update)

    fb_update = deserialise_pvAl(message)

    assert fb_update.state == AlarmState.AlarmState.UNDEFINED
    assert fb_update.severity == AlarmSeverity.AlarmSeverity.UNDEFINED
    assert fb_update.ca_state == CAAlarmState.CAAlarmState.UDF


def test_serialise_ca_connected():
    pv_name = "some_pv"
    serialiser = pvAl_Serialiser(pv_name)
    message, timestamp = serialiser.ca_conn_serialise(pv_name, "connected")

    fb_update = deserialise_pvAl(message)

    assert fb_update.source_name == pv_name
    assert datetime.now(tz=timezone.utc) - fb_update.timestamp < timedelta(seconds=1)
    assert fb_update.state == AlarmState.AlarmState.UNDEFINED
    assert fb_update.severity == AlarmSeverity.AlarmSeverity.UNDEFINED
    assert fb_update.ca_state == CAAlarmState.CAAlarmState.UDF


def test_repeated_alarm():
    pv_name = "some_pv"
    serialiser = pvAl_Serialiser(pv_name)
    serialiser.ca_conn_serialise(pv_name, "connected")
    message, timestamp = serialiser.ca_conn_serialise(pv_name, "connected")

    assert message is None
    assert timestamp is None
