import time

import numpy as np
import p4p.client.thread
import pytest
from numpy.typing import NDArray
from p4p.client.thread import Cancelled, Disconnected, Finished, RemoteError
from p4p.nt import NTScalar
from streaming_data_types.epics_connection_ep01 import ConnectionInfo, deserialise_ep01

from forwarder.update_handlers.ep01_serialiser import (
    ep01_CASerialiser,
    ep01_PVASerialiser,
)


def _test_serialise_start(pv_name, serialiser):
    message, timestamp = serialiser.start_state_serialise()

    fb_update = deserialise_ep01(message)

    assert fb_update.source_name == pv_name
    assert abs(fb_update.timestamp - (time.time() * 1e9)) / 1e9 < 0.5
    assert fb_update.status == ConnectionInfo.NEVER_CONNECTED


def _create_value_update(reference_timestamp):
    test_data: NDArray = np.array([-3, -2, -1]).astype(np.int32)
    update = NTScalar("ai").wrap(test_data)
    update.timeStamp.secondsPastEpoch = 0
    update.timeStamp.nanoseconds = reference_timestamp
    return update


def test_serialise_pva_start():
    pv_name = "some_pv"
    serialiser = ep01_PVASerialiser(pv_name)

    return _test_serialise_start(pv_name, serialiser)


def test_if_disconnected_and_never_connected_returns_none():
    serialiser = ep01_PVASerialiser("some_pv")

    message, timestamp = serialiser.serialise(p4p.client.thread.Disconnected())

    assert message is None
    assert timestamp is None


def test_serialise_pva_value():
    reference_timestamp = 10
    update = _create_value_update(reference_timestamp)

    pv_name = "some_pv"
    serialiser = ep01_PVASerialiser(pv_name)
    message, timestamp = serialiser.serialise(update)

    fb_update = deserialise_ep01(message)

    assert fb_update.source_name == pv_name
    assert fb_update.timestamp == reference_timestamp
    assert fb_update.status == ConnectionInfo.CONNECTED


def test_if_state_unchanged_then_message_is_none():
    reference_timestamp = 10
    update = _create_value_update(reference_timestamp)
    pv_name = "some_pv"
    serialiser = ep01_PVASerialiser(pv_name)
    # First update
    serialiser.serialise(update)

    # Resend the same update
    message, timestamp = serialiser.serialise(update)

    assert message is None
    assert timestamp is None


@pytest.mark.parametrize(
    "exception,state_enum",
    [
        (Disconnected(), ConnectionInfo.DISCONNECTED),
        (Cancelled(), ConnectionInfo.CANCELLED),
        (Finished(), ConnectionInfo.FINISHED),
        (RemoteError(), ConnectionInfo.REMOTE_ERROR),
    ],
)
def test_connected_to_exception_transition(exception, state_enum):
    pv_name = "some_pv"
    serialiser = ep01_PVASerialiser(pv_name)

    # First prime with a value update
    serialiser.serialise(_create_value_update(123))

    # Now try the exception
    message, timestamp = serialiser.serialise(exception)

    fb_update = deserialise_ep01(message)

    assert fb_update.source_name == pv_name
    assert abs(fb_update.timestamp - (time.time() * 1e9)) / 1e9 < 0.5
    assert fb_update.status == state_enum


@pytest.mark.parametrize(
    "exception,state_enum",
    [
        (Disconnected(), ConnectionInfo.DISCONNECTED),
        (Cancelled(), ConnectionInfo.CANCELLED),
        (Finished(), ConnectionInfo.FINISHED),
        (RemoteError(), ConnectionInfo.REMOTE_ERROR),
    ],
)
def test_if_never_connected_then_exceptions_returns_none(exception, state_enum):
    pv_name = "some_pv"
    serialiser = ep01_PVASerialiser(pv_name)

    message, timestamp = serialiser.serialise(exception)

    assert message is None
    assert timestamp is None


def test_serialise_nonsense_returns_unknown():
    pv_name = "some_pv"
    serialiser = ep01_PVASerialiser(pv_name)
    # First prime with a value update
    serialiser.serialise(_create_value_update(123))

    message, timestamp = serialiser.serialise("nonsense")

    fb_update = deserialise_ep01(message)

    assert fb_update.source_name == pv_name
    assert abs(fb_update.timestamp - (time.time() * 1e9)) / 1e9 < 0.5
    assert fb_update.status == ConnectionInfo.UNKNOWN


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
    assert fb_update.status == ConnectionInfo.CONNECTED


def test_serialise_ca_unknown():
    pv_name = "some_pv"
    serialiser = ep01_CASerialiser(pv_name)
    message, timestamp = serialiser.conn_serialise(pv_name, "3.15")

    fb_update = deserialise_ep01(message)

    assert fb_update.source_name == pv_name
    assert abs(fb_update.timestamp - (time.time() * 1e9)) / 1e9 < 0.5
    assert fb_update.status == ConnectionInfo.UNKNOWN
