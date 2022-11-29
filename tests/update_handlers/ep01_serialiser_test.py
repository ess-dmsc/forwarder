import time

import numpy as np
from p4p.client.thread import Disconnected
from p4p.nt import NTScalar
from streaming_data_types.epics_connection_ep01 import EventType, deserialise_ep01

from forwarder.update_handlers.ep01_serialiser import (
    ep01_CASerialiser,
    ep01_PVASerialiser,
)


def _test_serialise_start(pv_name, serialiser):
    message, timestamp = serialiser.start_state_serialise()

    fb_update = deserialise_ep01(message)

    assert fb_update.source_name == pv_name
    assert abs(fb_update.timestamp - (time.time() * 1e9)) / 1e9 < 0.5
    assert fb_update.type == EventType.EventType.NEVER_CONNECTED


def test_serialise_pva_start():
    pv_name = "some_pv"
    serialiser = ep01_PVASerialiser(pv_name)

    return _test_serialise_start(pv_name, serialiser)


def test_serialise_pva_value():
    test_data = np.array([-3, -2, -1]).astype(np.int32)
    reference_timestamp = 10
    update = NTScalar("ai").wrap(test_data)
    update.timeStamp.secondsPastEpoch = 0
    update.timeStamp.nanoseconds = reference_timestamp

    pv_name = "some_pv"
    serialiser = ep01_PVASerialiser(pv_name)
    message, timestamp = serialiser.serialise(update)

    fb_update = deserialise_ep01(message)

    assert fb_update.source_name == pv_name
    assert fb_update.timestamp == reference_timestamp
    assert fb_update.type == EventType.EventType.CONNECTED

    message, timestamp = serialiser.serialise(update)

    assert message is None
    assert timestamp is None


def test_serialise_pva_disconnected():
    pv_name = "some_pv"
    serialiser = ep01_PVASerialiser(pv_name)
    message, timestamp = serialiser.serialise(Disconnected())

    fb_update = deserialise_ep01(message)

    assert fb_update.source_name == pv_name
    assert abs(fb_update.timestamp - (time.time() * 1e9)) / 1e9 < 0.5
    assert fb_update.type == EventType.EventType.DISCONNECTED


def test_serialise_pva_unknown():
    pv_name = "some_pv"
    serialiser = ep01_PVASerialiser(pv_name)
    message, timestamp = serialiser.serialise(3.14)

    fb_update = deserialise_ep01(message)

    assert fb_update.source_name == pv_name
    assert abs(fb_update.timestamp - (time.time() * 1e9)) / 1e9 < 0.5
    assert fb_update.type == EventType.EventType.UNKNOWN


def test_serialise_ca_start():
    pv_name = "some_pv"
    serialiser = ep01_CASerialiser(pv_name)

    return _test_serialise_start(pv_name, serialiser)


def test_serialise_ca_value():
    pv_name = "some_pv"
    serialiser = ep01_CASerialiser(pv_name)

    message, timestamp = serialiser.serialise(1)

    assert message is None
    assert timestamp is None


def test_serialise_ca_connected():
    pv_name = "some_pv"
    serialiser = ep01_CASerialiser(pv_name)
    message, timestamp = serialiser.conn_serialise(pv_name, "connected")

    fb_update = deserialise_ep01(message)

    assert fb_update.source_name == pv_name
    assert abs(fb_update.timestamp - (time.time() * 1e9)) / 1e9 < 0.5
    assert fb_update.type == EventType.EventType.CONNECTED


def test_serialise_ca_unknown():
    pv_name = "some_pv"
    serialiser = ep01_CASerialiser(pv_name)
    message, timestamp = serialiser.conn_serialise(pv_name, "3.15")

    fb_update = deserialise_ep01(message)

    assert fb_update.source_name == pv_name
    assert abs(fb_update.timestamp - (time.time() * 1e9)) / 1e9 < 0.5
    assert fb_update.type == EventType.EventType.UNKNOWN
