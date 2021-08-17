from datetime import datetime
from typing import Tuple, Union

import numpy as np
import p4p
from caproto import Message as CA_Message
from streaming_data_types.sample_environment_senv import serialise_senv


class nttable_senv_Serialiser:
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._msg_counter = -1

    def serialise(
        self, update: Union[p4p.Value, CA_Message], **unused
    ) -> Tuple[bytes, int]:
        if isinstance(update, CA_Message):
            raise RuntimeError(
                "nttable_senv_Serialiser is unable to process channel access data."
            )
        if update.getID() != "epics:nt/NTTable:1.0":
            raise RuntimeError(
                f'Unable to process EPICS updates of type: "{update.getID()}".'
            )
        column_headers = update.labels
        if "value" not in column_headers or "timestamp" not in column_headers:
            raise RuntimeError(
                f'Unable to find required columns ("value", "timestamp") in NTTable. Found the columns {column_headers} instead.'
            )
        tables = update.value.items()
        values = tables[column_headers.index("value")][1]
        if np.issubdtype(values.dtype, np.floating):
            values = values.round().astype(np.int64)
        timestamps = tables[column_headers.index("timestamp")][1]
        self._msg_counter += 1
        origin_timestamp = timestamps[0]
        message_timestamp = datetime.fromtimestamp(origin_timestamp / 1e9)
        delta_time = timestamps[1] - timestamps[0]
        return (
            serialise_senv(
                name=self._source_name,
                value_timestamps=timestamps,
                values=values,
                timestamp=message_timestamp,
                message_counter=self._msg_counter,
                sample_ts_delta=delta_time,
                channel=0,
            ),
            origin_timestamp,
        )
