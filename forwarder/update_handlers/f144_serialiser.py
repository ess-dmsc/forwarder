from typing import Tuple, Union

import numpy as np
import p4p
from caproto import Message as CA_Message
from streaming_data_types.logdata_f144 import serialise_f144

from forwarder.epics_to_serialisable_types import (
    numpy_type_from_caproto_type,
    numpy_type_from_p4p_type,
)
from forwarder.kafka.kafka_helpers import seconds_to_nanoseconds
from forwarder.update_handlers.schema_serialisers import CASerialiser, PVASerialiser


def _extract_pva_data(update: p4p.Value) -> np.ndarray:
    if update.getID() == "epics:nt/NTEnum:1.0":
        return update.value.index
    data_type = numpy_type_from_p4p_type[update.type()["value"][-1]]
    return np.squeeze(np.array(update.value)).astype(data_type)


def _extract_ca_data(update: CA_Message) -> np.ndarray:
    data_type = numpy_type_from_caproto_type[update.data_type]
    data = update.data
    if type(data) is not np.ndarray:
        data = np.array(data).astype(data_type)
    else:
        data = data.astype(np.dtype(data.dtype.str.strip("<>=")))
    return np.squeeze(data)


def _serialise(
    source_name: str,
    value: np.ndarray,
    timestamp: int,
) -> Tuple[bytes, int]:
    return (
        serialise_f144(source_name, value, timestamp),
        timestamp,
    )


class f144_CASerialiser(CASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name

    def serialise(
        self, update: CA_Message, **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        timestamp = seconds_to_nanoseconds(update.metadata.timestamp)
        value = _extract_ca_data(update)
        return _serialise(self._source_name, value, timestamp)

    def conn_serialise(self, pv: str, state: str) -> Tuple[None, None]:
        return None, None


class f144_PVASerialiser(PVASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name

    def serialise(
        self, update: Union[p4p.Value, RuntimeError]
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        if isinstance(update, RuntimeError):
            return None, None
        value = _extract_pva_data(update)
        timestamp = (
            update.timeStamp.secondsPastEpoch * 1_000_000_000
        ) + update.timeStamp.nanoseconds
        return _serialise(self._source_name, value, timestamp)
