from typing import Tuple, Union

import p4p
from caproto import Message as CA_Message
from streaming_data_types.alarm_al00 import Severity, serialise_al00

from forwarder.epics_to_serialisable_types import epics_alarm_severity_to_al00
from forwarder.kafka.kafka_helpers import seconds_to_nanoseconds
from forwarder.update_handlers.schema_serialisers import CASerialiser, PVASerialiser


def _serialise(
    source_name: str,
    timestamp_ns: int,
    severity: Severity,
    message: str,
) -> Tuple[bytes, int]:
    return (
        serialise_al00(source_name, timestamp_ns, severity, message),
        timestamp_ns,
    )


class al00_CASerialiser(CASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name

    def serialise(
        self, update: CA_Message, **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        timestamp = seconds_to_nanoseconds(update.metadata.timestamp)
        severity = epics_alarm_severity_to_al00[update.metadata.severity]
        message = update.metadata.status.name
        return _serialise(self._source_name, timestamp, severity, message)

    def conn_serialise(self, pv: str, state: str) -> Tuple[None, None]:
        return None, None


class al00_PVASerialiser(PVASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name

    def serialise(
        self, update: Union[p4p.Value, RuntimeError]
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        if isinstance(update, RuntimeError):
            return None, None
        timestamp = (
            update.timeStamp.secondsPastEpoch * 1_000_000_000
        ) + update.timeStamp.nanoseconds
        severity = epics_alarm_severity_to_al00[update.alarm.severity]
        message = update.alarm.message
        return _serialise(self._source_name, timestamp, severity, message)
