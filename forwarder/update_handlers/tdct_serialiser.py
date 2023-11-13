from typing import Tuple, Union

import numpy as np
import p4p
from caproto import Message as CA_Message
from numpy.typing import NDArray
from streaming_data_types.timestamps_tdct import serialise_tdct

from forwarder.epics_to_serialisable_types import (
    numpy_type_from_caproto_type,
    numpy_type_from_p4p_type,
)
from forwarder.kafka.kafka_helpers import seconds_to_nanoseconds
from forwarder.update_handlers.schema_serialisers import CASerialiser, PVASerialiser


def _extract_ca_data(update: CA_Message) -> np.ndarray:
    data_type = numpy_type_from_caproto_type[update.data_type]
    return np.squeeze(np.array(update.data)).astype(data_type)


class tdct_CASerialiser(CASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._msg_counter = -1

    def _serialise(self, value_arr: np.ndarray, origin_time: int) -> Tuple[bytes, int]:
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

    def serialise(
        self, update: CA_Message, **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        if update.data.size == 0:
            return None, None
        origin_time = seconds_to_nanoseconds(update.metadata.timestamp)
        value_arr = _extract_ca_data(update)
        return self._serialise(value_arr, origin_time)

    def conn_serialise(self, pv: str, state: str) -> Tuple[None, None]:
        return None, None


class tdct_PVASerialiser(PVASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._msg_counter = -1

    def _serialise(self, value_arr: np.ndarray, origin_time: int) -> Tuple[bytes, int]:
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

    def serialise(
        self, update: Union[p4p.Value, RuntimeError], **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        if isinstance(update, RuntimeError):
            return None, None
        origin_time = (
            update.timeStamp.secondsPastEpoch * 1_000_000_000
        ) + update.timeStamp.nanoseconds

        allowed_types = ["epics:nt/NTScalar:1.0", "epics:nt/NTScalarArray:1.0"]
        if update.getID() not in allowed_types:
            raise RuntimeError(
                f'Unable to extract TDC data from EPICS type: "{update.getID()}"'
            )
        if update.value is None:
            return None, None
        try:
            if update.value.size == 0:
                return None, None
        except AttributeError:
            pass
        data_type = numpy_type_from_p4p_type[update.type()["value"][-1]]
        value_arr: NDArray = np.squeeze(np.array(update.value)).astype(data_type)
        return self._serialise(value_arr, origin_time)
