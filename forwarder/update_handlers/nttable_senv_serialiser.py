from streaming_data_types.sample_environment_senv import serialise_senv
from typing import Union
import p4p
from caproto import ReadNotifyResponse

class nttable_senv_Serialiser:
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._msg_counter = -1

    def serialise(self, update: Union[p4p.Value, ReadNotifyResponse], **unused) -> bytes:
        if isinstance(update, ReadNotifyResponse):
            raise RuntimeError("nttable_senv_Serialiser is unable to process channel access data.")
        # timestamp = (update.timeStamp.secondsPastEpoch * 1_000_000_000) + update.timeStamp.nanoseconds
        self._msg_counter += 1
        # return serialise_senv(
        #     name=self._source_name, timestamps=unix_epoch_timestamps_ns.astype(np.uint64),
        #     sequence_counter=self._msg_counter
        # )
        return b"aaaaaaaaaaaaaa"