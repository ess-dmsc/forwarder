from datetime import datetime, timezone

import numpy as np
from p4p import Value
from p4p.nt import NTTable
from streaming_data_types.sample_environment_senv import deserialise_senv

from forwarder.update_handlers.nttable_senv_serialiser import nttable_senv_PVASerialiser


def test_serialise_nttable():
    values = np.arange(-50, 50, dtype=np.int16)
    timestamps = np.arange(50, 150, dtype=np.uint64)
    first_timestamp = timestamps[0] / 1e9

    table = NTTable.buildType(
        columns=[
            ("column0", "ah"),
            ("column1", "aL"),
        ]
    )

    update = Value(
        table,
        {
            "labels": ["value", "timestamp"],
            "value": {"column0": values, "column1": timestamps},
        },
    )

    pv_name = "some_pv"
    serialiser = nttable_senv_PVASerialiser(pv_name)
    message, timestamp = serialiser.serialise(update)

    fb_update = deserialise_senv(message)

    assert fb_update.name == pv_name
    assert np.array_equal(fb_update.values, values)
    assert fb_update.values.dtype == values.dtype
    assert np.array_equal(fb_update.value_ts, timestamps)
    assert fb_update.timestamp == datetime.fromtimestamp(
        float(first_timestamp), tz=timezone.utc
    )
    assert fb_update.message_counter == 0
