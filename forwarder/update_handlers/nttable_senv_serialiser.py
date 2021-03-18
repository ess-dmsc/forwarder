from streaming_data_types.sample_environment_senv import serialise_senv
from typing import Union, Tuple
import p4p
from caproto import ReadNotifyResponse
from datetime import datetime


class nttable_senv_Serialiser:
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._msg_counter = -1

    def serialise(self, update: Union[p4p.Value, ReadNotifyResponse], **unused) -> Tuple[bytes, int]:
        if isinstance(update, ReadNotifyResponse):
            raise RuntimeError("nttable_senv_Serialiser is unable to process channel access data.")
        if update.getID() != "epics:nt/NTTable:1.0":
            raise RuntimeError(f"Unable to process EPICS updates of type: \"{update.getID()}\".")
        column_headers = update.value.keys()
        if "value" not in column_headers or "timestamp" not in column_headers:
            raise RuntimeError(f"Unable to find required columns (\"value\", \"timestamp\") in NTTable. Found the columns {column_headers} instead.")
        values = update.value.value
        timestamps = update.value.timestamp
        self._msg_counter += 1
        origin_timestamp = timestamps[0]
        message_timestamp = datetime.fromtimestamp(origin_timestamp / 1e9)
        delta_time = timestamps[1] - timestamps[0]
        return serialise_senv(
            name=self._source_name, value_timestamps=timestamps, values=values, timestamp=message_timestamp,
            message_counter=self._msg_counter, sample_ts_delta=delta_time, channel=0
        ), origin_timestamp
