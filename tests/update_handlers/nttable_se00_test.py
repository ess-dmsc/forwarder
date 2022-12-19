import numpy as np
from p4p import Value
from p4p.nt import NTTable
from streaming_data_types.array_1d_se00 import deserialise_se00

from forwarder.update_handlers.nttable_se00_serialiser import nttable_se00_PVASerialiser


def test_serialise_nttable_se00():
    values = np.arange(-50, 50, 11, dtype=np.float32)
    timestamps = np.arange(50, 150, dtype=np.uint64)
    table = NTTable.buildType(
        columns=[
            ("column0", "af"),
            ("column1", "aL"),
        ],
    )
    update = Value(
        table,
        {
            "labels": ["value", "timestamp"],
            "value": {"column0": values, "column1": timestamps},
        },
    )

    pv_name = "some_pv"
    serialiser = nttable_se00_PVASerialiser(pv_name)
    message, timestamp = serialiser.serialise(update)

    fb_update = deserialise_se00(message)

    assert fb_update.name == pv_name
    assert np.array_equal(fb_update.values, values)
    assert fb_update.values.dtype == values.dtype
    assert np.array_equal(fb_update.value_ts, timestamps)
    assert fb_update.timestamp_unix_ns == timestamps[0]
    assert fb_update.message_counter == 0
