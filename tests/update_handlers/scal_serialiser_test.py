import numpy as np
from p4p.client.thread import Disconnected
from streaming_data_types.epics_pv_scalar_data_scal import deserialise_scal

from forwarder.update_handlers.scal_serialiser import scal_Serialiser
from p4p.nt import NTScalar
from datetime import datetime, timezone
from caproto import ReadNotifyResponse, TimeStamp, timestamp_to_epics, ChannelType


def test_serialise_pva_single_value():
    test_value = 123456
    reference_time = datetime.now(tz=timezone.utc)
    update = NTScalar("i").wrap(test_value)
    update.timeStamp.secondsPastEpoch = int(reference_time.timestamp())
    update.timeStamp.nanoseconds = int(
        (reference_time.timestamp() - int(reference_time.timestamp())) * 1e9
    )

    pv_name = "some_pv"
    serialiser = scal_Serialiser(pv_name)
    message, timestamp = serialiser.pva_serialise(update)

    fb_update = deserialise_scal(message)

    assert fb_update.source_name == pv_name
    assert fb_update.timestamp == reference_time
    assert fb_update.value == test_value


def test_serialise_pva_array_value():
    test_value = np.arange(10, 20, dtype=np.int32)
    reference_time = datetime.now(tz=timezone.utc)
    update = NTScalar("ai").wrap(test_value)
    update.timeStamp.secondsPastEpoch = int(reference_time.timestamp())
    update.timeStamp.nanoseconds = int(
        (reference_time.timestamp() - int(reference_time.timestamp())) * 1e9
    )

    pv_name = "some_pv"
    serialiser = scal_Serialiser(pv_name)
    message, timestamp = serialiser.pva_serialise(update)

    fb_update = deserialise_scal(message)

    assert fb_update.source_name == pv_name
    assert fb_update.timestamp == reference_time
    assert (fb_update.value == test_value).all()


def test_serialise_ca_single_value():
    pv_name = "some_pv"
    reference_time = datetime.now(tz=timezone.utc)
    serialiser = scal_Serialiser(pv_name)

    metadata = (
        0,
        0,
        TimeStamp(*timestamp_to_epics(reference_time.replace(tzinfo=None))),
    )

    test_value = 1.2345

    update = ReadNotifyResponse(
        [
            test_value,
        ],
        ChannelType.TIME_DOUBLE,
        1,
        1,
        1,
        metadata=metadata,
    )

    message, timestamp = serialiser.ca_serialise(update)

    fb_update = deserialise_scal(message)

    assert fb_update.source_name == pv_name
    assert fb_update.timestamp == reference_time
    assert fb_update.value == test_value


def test_serialise_ca_array_value():
    pv_name = "some_pv"
    reference_time = datetime.now(tz=timezone.utc)
    serialiser = scal_Serialiser(pv_name)

    metadata = (
        0,
        0,
        TimeStamp(*timestamp_to_epics(reference_time.replace(tzinfo=None))),
    )

    test_value = np.linspace(1, 2)

    update = ReadNotifyResponse(
        [
            test_value,
        ],
        ChannelType.TIME_DOUBLE,
        len(test_value),
        1,
        1,
        metadata=metadata,
    )

    message, timestamp = serialiser.ca_serialise(update)

    fb_update = deserialise_scal(message)

    assert fb_update.source_name == pv_name
    assert fb_update.timestamp == reference_time
    assert (fb_update.value == test_value).all()


def test_serialise_pva_disconnected():
    pv_name = "some_pv"
    serialiser = scal_Serialiser(pv_name)
    message, timestamp = serialiser.pva_serialise(Disconnected())

    assert message is None
    assert timestamp is None


def test_serialise_ca_connected():
    pv_name = "some_pv"
    serialiser = scal_Serialiser(pv_name)
    message, timestamp = serialiser.ca_conn_serialise(pv_name, "connected")

    assert message is None
    assert timestamp is None
