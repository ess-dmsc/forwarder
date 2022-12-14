from typing import Tuple, Union

import p4p
from streaming_data_types.array_1d_se00 import serialise_se00

from forwarder.update_handlers.schema_serialisers import PVASerialiser


class nttable_se00_PVASerialiser(PVASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._msg_counter = -1

    def serialise(
        self, update: Union[p4p.Value, RuntimeError], **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        if isinstance(update, RuntimeError):
            return None, None
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
        timestamps = tables[column_headers.index("timestamp")][1]
        if len(timestamps) == 0:
            return None, None
        self._msg_counter += 1
        origin_timestamp = timestamps[0]
        message_timestamp = timestamps[0]
        delta_time = timestamps[1] - timestamps[0]
        return (
            serialise_se00(
                name=self._source_name,
                value_timestamps=timestamps,
                values=values,
                timestamp_unix_ns=message_timestamp,
                message_counter=self._msg_counter,
                sample_ts_delta=delta_time,
                channel=0,
            ),
            origin_timestamp,
        )
