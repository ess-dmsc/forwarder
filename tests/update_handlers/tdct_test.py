import numpy as np
from p4p.nt import NTScalar
from streaming_data_types.timestamps_tdct import deserialise_tdct

from forwarder.update_handlers.tdct_serialiser import PVA_tdct_Serialiser


def test_tdct_serialiser_handles_negative_relative_timestamps():
    input_relative_timestamps = np.array([-3, -2, -1]).astype(np.int32)
    # This is the timestamp of the PV update
    reference_timestamp = 10
    serialiser = PVA_tdct_Serialiser("tst_source")

    update = NTScalar("ai").wrap(input_relative_timestamps)
    update.timeStamp.secondsPastEpoch = 0
    update.timeStamp.nanoseconds = reference_timestamp

    message = serialiser.pva_serialise(update)

    published_data = deserialise_tdct(message[0])
    assert np.array_equal(
        published_data.timestamps, input_relative_timestamps + reference_timestamp
    ), "Expected the relative timestamps from the EPICS update to have been converted to unix timestamps"


def test_tdct_publisher_publishes_successfully_when_there_is_only_a_single_chopper_timestamp():
    # These are the values that would be in the array in the PV update
    input_relative_timestamps = 1
    # This is the timestamp of the PV update
    reference_timestamp = 10
    update = NTScalar("i").wrap(input_relative_timestamps)
    update.timeStamp.secondsPastEpoch = 0
    update.timeStamp.nanoseconds = reference_timestamp

    serialiser = PVA_tdct_Serialiser("tst_source")
    message = serialiser.pva_serialise(update)

    published_data = deserialise_tdct(message[0])
    assert np.array_equal(
        published_data.timestamps,
        np.atleast_1d(input_relative_timestamps + reference_timestamp),
    ), "Expected the relative timestamps from the EPICS update to have been converted to unix timestamps"
