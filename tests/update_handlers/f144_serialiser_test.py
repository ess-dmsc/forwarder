import numpy as np
import pytest
from p4p.client.thread import Cancelled, Disconnected, Finished, RemoteError
from p4p.nt import NTScalar
from streaming_data_types.logdata_f144 import ExtractedLogData, deserialise_f144

from forwarder.update_handlers.f144_serialiser import f144_PVASerialiser


@pytest.mark.parametrize(
    "reference_timestamp",
    [
        10,
        None,  # timestamp is optional in the schema
    ],
)
def test_serialise_pva_value(reference_timestamp):
    test_data = np.array([-3, -2, -1]).astype(np.int32)
    test_timestamp_seconds_past_epoch = 123
    update = NTScalar("ai").wrap(test_data)
    if reference_timestamp:
        update.timeStamp.secondsPastEpoch = test_timestamp_seconds_past_epoch
        update.timeStamp.nanoseconds = reference_timestamp

    pv_name = "some_pv"
    serialiser = f144_PVASerialiser(pv_name)
    message, _ = serialiser.serialise(update)
    fb_update: ExtractedLogData = deserialise_f144(message)

    assert fb_update.source_name == pv_name
    assert not reference_timestamp or (
        fb_update.timestamp_unix_ns
        == reference_timestamp + 1e9 * test_timestamp_seconds_past_epoch
    )
    assert np.array_equal(fb_update.value, test_data)


@pytest.mark.parametrize(
    "exception",
    [
        Disconnected(),
        Cancelled(),
        Finished(),
        RemoteError(),
    ],
)
def test_serialise_pva_exception(exception):
    pv_name = "some_pv"
    serialiser = f144_PVASerialiser(pv_name)

    assert (None, None) == serialiser.serialise(exception)


# def test_serialise_ca_value():
#     pv_name = "some_pv"
#     serialiser = f144_CASerialiser(pv_name)

#     message, timestamp = serialiser.serialise(1)

#     assert message is None
#     assert timestamp is None
