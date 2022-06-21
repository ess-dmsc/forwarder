from typing import Tuple, Union

import numpy as np
import p4p
from caproto import Message as CA_Message
from streaming_data_types.epics_pv_scalar_data_scal import serialise_scal

from forwarder.epics_to_serialisable_types import (
    numpy_type_from_caproto_type,
    numpy_type_from_p4p_type,
)
from datetime import datetime, timedelta, timezone


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


class scal_Serialiser:
    def __init__(self, source_name: str):
        self._source_name = source_name

    def _serialise(self, value, timestamp) -> Tuple[bytes, int]:
        return (
            serialise_scal(
                value=value, source_name=self._source_name, timestamp=timestamp
            ),
            timestamp.timestamp() / 1_000_000_000,
        )

    def pva_serialise(
        self, update: Union[p4p.Value, RuntimeError]
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        if isinstance(update, RuntimeError):
            return None, None
        value = _extract_pva_data(update)
        timestamp = datetime.fromtimestamp(
            update.timeStamp.secondsPastEpoch, tz=timezone.utc
        ) + timedelta(microseconds=update.timeStamp.nanoseconds / 1000)
        return self._serialise(value=value, timestamp=timestamp)

    def ca_serialise(
        self, update: CA_Message, **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        timestamp = datetime.fromtimestamp(update.metadata.timestamp, tz=timezone.utc)
        value = _extract_ca_data(update)
        return self._serialise(value=value, timestamp=timestamp)

    def ca_conn_serialise(self, pv: str, state: str) -> Tuple[None, None]:
        return None, None
