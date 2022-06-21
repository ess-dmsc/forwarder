import numpy as np
from p4p.client.thread import Disconnected
from streaming_data_types.epics_pv_conn_status_pvCn import deserialise_pvCn, ConnectionInfo

from forwarder.update_handlers.pvCn_serialiser import pvCn_Serialiser
from p4p.nt import NTScalar
from datetime import datetime, timedelta, timezone


def test_serialise_start():
    pv_name = "some_pv"
    serialiser = pvCn_Serialiser(pv_name)
    message, timestamp = serialiser.start_state_serialise()

    fb_update = deserialise_pvCn(message)

    assert fb_update.source_name == pv_name
    assert datetime.now(tz=timezone.utc) - fb_update.timestamp < timedelta(seconds=1)
    assert fb_update.status == ConnectionInfo.ConnectionInfo.NEVER_CONNECTED


def test_serialise_pva_value():
    test_data = np.array([-3, -2, -1]).astype(np.int32)
    reference_nanoseconds = 10 * 1000
    update = NTScalar("ai").wrap(test_data)
    update.timeStamp.secondsPastEpoch = 0
    update.timeStamp.nanoseconds = reference_nanoseconds

    pv_name = "some_pv"
    serialiser = pvCn_Serialiser(pv_name)
    message, timestamp = serialiser.pva_serialise(update)

    fb_update = deserialise_pvCn(message)

    assert fb_update.source_name == pv_name
    assert fb_update.timestamp == datetime.fromtimestamp(0, tz=timezone.utc) + timedelta(microseconds=reference_nanoseconds / 1000)
    assert fb_update.status == ConnectionInfo.ConnectionInfo.CONNECTED

    message, timestamp = serialiser.pva_serialise(update)

    assert message is None
    assert timestamp is None


def test_serialise_pva_disconnected():
    pv_name = "some_pv"
    serialiser = pvCn_Serialiser(pv_name)
    message, timestamp = serialiser.pva_serialise(Disconnected())

    fb_update = deserialise_pvCn(message)

    assert fb_update.source_name == pv_name
    assert datetime.now(tz=timezone.utc) - fb_update.timestamp < timedelta(seconds=1)
    assert fb_update.status == ConnectionInfo.ConnectionInfo.DISCONNECTED


def test_serialise_pva_unknown():
    pv_name = "some_pv"
    serialiser = pvCn_Serialiser(pv_name)
    message, timestamp = serialiser.pva_serialise(3.14)

    fb_update = deserialise_pvCn(message)

    assert fb_update.source_name == pv_name
    assert datetime.now(tz=timezone.utc) - fb_update.timestamp < timedelta(seconds=1)
    assert fb_update.status == ConnectionInfo.ConnectionInfo.UNKNOWN


def test_serialise_ca_value():
    pv_name = "some_pv"
    serialiser = pvCn_Serialiser(pv_name)

    message, timestamp = serialiser.ca_serialise(1)

    assert message is None
    assert timestamp is None


def test_serialise_ca_connected():
    pv_name = "some_pv"
    serialiser = pvCn_Serialiser(pv_name)
    message, timestamp = serialiser.ca_conn_serialise(pv_name, "connected")

    fb_update = deserialise_pvCn(message)

    assert fb_update.source_name == pv_name
    assert datetime.now(tz=timezone.utc) - fb_update.timestamp < timedelta(seconds=1)
    assert fb_update.status == ConnectionInfo.ConnectionInfo.CONNECTED


def test_serialise_ca_unknown():
    pv_name = "some_pv"
    serialiser = pvCn_Serialiser(pv_name)
    message, timestamp = serialiser.ca_conn_serialise(pv_name, 3.15)

    fb_update = deserialise_pvCn(message)

    assert fb_update.source_name == pv_name
    assert datetime.now(tz=timezone.utc) - fb_update.timestamp < timedelta(seconds=1)
    assert fb_update.status == ConnectionInfo.ConnectionInfo.UNKNOWN
