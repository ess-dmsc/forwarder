from p4p.nt import NTTable
from p4p import Value
from forwarder.update_handlers.nttable_senv_serialiser import nttable_senv_Serialiser
from streaming_data_types.sample_environment_senv import deserialise_senv
import numpy as np
from datetime import datetime


def test_serialise_nttable():
    values = np.arange(-50, 50, dtype=np.int16)
    timestamps = np.arange(50, 150, dtype=np.uint64)

    table = NTTable.buildType(columns=[
        ('value', 'ah'),
        ('timestamp', 'aL'),
    ])

    update = Value(table, {"labels": ["value", "timestamp"],
                           "value": {"value": values, "timestamp": timestamps}})

    pv_name = "some_pv"
    serialiser = nttable_senv_Serialiser(pv_name)
    message, timestamp = serialiser.serialise(update)

    fb_update = deserialise_senv(message)

    assert fb_update.name == pv_name
    assert np.array_equal(fb_update.values, values)
    assert fb_update.values.dtype == values.dtype
    assert np.array_equal(fb_update.value_ts, timestamps)
    assert fb_update.timestamp == datetime.fromtimestamp(timestamps[0] / 1e9)
    assert fb_update.message_counter == 0
