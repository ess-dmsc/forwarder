import numpy as np
from p4p.client.thread import Disconnected
from streaming_data_types.epics_connection_info_ep00 import deserialise_ep00, EventType

from forwarder.update_handlers.ep00_serialiser import ep00_Serialiser
import time
from p4p.nt import NTScalar


def test_serialise_start():
    pv_name = "some_pv"
    serialiser = ep00_Serialiser(pv_name)
    message, timestamp = serialiser.start_state_serialise()

    fb_update = deserialise_ep00(message)

    assert fb_update.source_name == pv_name
    assert abs(fb_update.timestamp - (time.time() * 1e9)) / 1e9 < 0.5
    assert fb_update.type == EventType.EventType.NEVER_CONNECTED

def test_serialise_pva_value():
    test_data = np.array([-3, -2, -1]).astype(np.int32)
    reference_timestamp = 10
    update = NTScalar("ai").wrap(test_data)
    update.timeStamp.secondsPastEpoch = 0
    update.timeStamp.nanoseconds = reference_timestamp

    pv_name = "some_pv"
    serialiser = ep00_Serialiser(pv_name)
    message, timestamp = serialiser.pva_serialise(update)

    fb_update = deserialise_ep00(message)

    assert fb_update.source_name == pv_name
    assert fb_update.timestamp == reference_timestamp
    assert fb_update.type == EventType.EventType.CONNECTED

    message, timestamp = serialiser.pva_serialise(update)

    assert message is None
    assert timestamp is None


def test_serialise_pva_disconnected():
    pv_name = "some_pv"
    serialiser = ep00_Serialiser(pv_name)
    message, timestamp = serialiser.pva_serialise(Disconnected())

    fb_update = deserialise_ep00(message)

    assert fb_update.source_name == pv_name
    assert abs(fb_update.timestamp - (time.time() * 1e9)) / 1e9 < 0.5
    assert fb_update.type == EventType.EventType.DISCONNECTED

def test_serialise_pva_unknown():
    pv_name = "some_pv"
    serialiser = ep00_Serialiser(pv_name)
    message, timestamp = serialiser.pva_serialise(3.14)

    fb_update = deserialise_ep00(message)

    assert fb_update.source_name == pv_name
    assert abs(fb_update.timestamp - (time.time() * 1e9)) / 1e9 < 0.5
    assert fb_update.type == EventType.EventType.UNKNOWN


def test_serialise_ca_value():
    pv_name = "some_pv"
    serialiser = ep00_Serialiser(pv_name)

    message, timestamp = serialiser.ca_serialise(1)

    assert message is None
    assert timestamp is None


def test_serialise_ca_connected():
    pv_name = "some_pv"
    serialiser = ep00_Serialiser(pv_name)
    message, timestamp = serialiser.ca_conn_serialise(pv_name, "connected")

    fb_update = deserialise_ep00(message)

    assert fb_update.source_name == pv_name
    assert abs(fb_update.timestamp - (time.time() * 1e9)) / 1e9 < 0.5
    assert fb_update.type == EventType.EventType.CONNECTED

def test_serialise_ca_unknown():
    pv_name = "some_pv"
    serialiser = ep00_Serialiser(pv_name)
    message, timestamp = serialiser.ca_conn_serialise(pv_name, 3.15)

    fb_update = deserialise_ep00(message)

    assert fb_update.source_name == pv_name
    assert abs(fb_update.timestamp - (time.time() * 1e9)) / 1e9 < 0.5
    assert fb_update.type == EventType.EventType.UNKNOWN
