from streaming_data_types.timestamps_tdct import serialise_tdct
import p4p
from typing import Union, Tuple
from caproto import ReadNotifyResponse
import numpy as np
from forwarder.epics_to_serialisable_types import (
    numpy_type_from_p4p_type,
    numpy_type_from_caproto_type,
)
from forwarder.kafka.kafka_helpers import seconds_to_nanoseconds


def _extract_pva_data(update: p4p.Value):
    allowed_types = ["epics:nt/NTScalar:1.0", "epics:nt/NTScalarArray:1.0"]
    if update.getID() not in allowed_types:
        raise RuntimeError(
            f'Unable to extract TDC data from EPICS type: "{update.getID()}"'
        )
    data_type = numpy_type_from_p4p_type[update.type()["value"][-1]]
    return np.squeeze(np.array(update.value)).astype(data_type)


def _extract_ca_data(update: ReadNotifyResponse):
    data_type = numpy_type_from_caproto_type[update.data_type]
    return np.squeeze(np.array(update.data)).astype(data_type)


class tdct_Serialiser:
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._msg_counter = -1

    def serialise(
        self, update: Union[p4p.Value, ReadNotifyResponse], **unused
    ) -> Tuple[bytes, int]:
        if isinstance(update, p4p.Value):
            origin_time = (
                update.timeStamp.secondsPastEpoch * 1_000_000_000
            ) + update.timeStamp.nanoseconds
            value_arr = _extract_pva_data(update)
        elif isinstance(update, ReadNotifyResponse):
            origin_time = seconds_to_nanoseconds(update.metadata.timestamp)
            value_arr = _extract_ca_data(update)
        timestamps = value_arr + origin_time
        self._msg_counter += 1
        return (
            serialise_tdct(
                name=self._source_name,
                timestamps=timestamps.astype(np.uint64),
                sequence_counter=self._msg_counter,
            ),
            origin_time,
        )
