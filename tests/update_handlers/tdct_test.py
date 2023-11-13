import numpy as np
import pytest
from numpy.typing import NDArray
from p4p.nt import NTScalar
from streaming_data_types.timestamps_tdct import deserialise_tdct

from forwarder.common import EpicsProtocol
from forwarder.update_handlers.tdct_serialiser import tdct_PVASerialiser

from .pva_update_handler_test import update_handler_publishes_alarm_update

# Set marks required by the 'context' fixture.
pytestmark = [pytest.mark.epics_protocol(EpicsProtocol.PVA), pytest.mark.schema("tdct")]


def test_tdct_serialiser_handles_positive_relative_timestamps():
    input_relative_timestamps: NDArray = np.array([10, 11, 12]).astype(np.int32)
    # This is the timestamp of the PV update
    reference_timestamp = 10
    serialiser = tdct_PVASerialiser("tst_source")

    update = NTScalar("ai").wrap(input_relative_timestamps)
    update.timeStamp.secondsPastEpoch = 0
    update.timeStamp.nanoseconds = reference_timestamp

    message = serialiser.serialise(update)

    published_data = deserialise_tdct(message[0])
    assert np.array_equal(
        published_data.timestamps, input_relative_timestamps + reference_timestamp
    ), "Expected the relative timestamps from the EPICS update to have been converted to unix timestamps"


def test_tdct_serialiser_handles_negative_relative_timestamps():
    input_relative_timestamps: NDArray = np.array([-3, -2, -1]).astype(np.int32)
    # This is the timestamp of the PV update
    reference_timestamp = 10
    serialiser = tdct_PVASerialiser("tst_source")

    update = NTScalar("ai").wrap(input_relative_timestamps)
    update.timeStamp.secondsPastEpoch = 0
    update.timeStamp.nanoseconds = reference_timestamp

    message = serialiser.serialise(update)

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

    serialiser = tdct_PVASerialiser("tst_source")
    message = serialiser.serialise(update)

    published_data = deserialise_tdct(message[0])
    assert np.array_equal(
        published_data.timestamps,
        np.atleast_1d(input_relative_timestamps + reference_timestamp),
    ), "Expected the relative timestamps from the EPICS update to have been converted to unix timestamps"


def test_tdct_serialiser_handles_empty_array():
    input_relative_timestamps: NDArray = np.array([]).astype(np.int32)
    # This is the timestamp of the PV update
    reference_timestamp = 10
    serialiser = tdct_PVASerialiser("tst_source")

    update = NTScalar("ai").wrap(input_relative_timestamps)
    update.timeStamp.secondsPastEpoch = 0
    update.timeStamp.nanoseconds = reference_timestamp

    message = serialiser.serialise(update)

    assert message == (
        None,
        None,
    ), "Expected serialiser to skip empty timestamp array from the EPICS update"


def test_update_handler_publishes_tdct_alarm_update(context, producer, pv_source_name):
    return update_handler_publishes_alarm_update(context, producer, pv_source_name)
