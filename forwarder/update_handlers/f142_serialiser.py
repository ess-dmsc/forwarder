from typing import Tuple, Union

import numpy as np
import p4p
from caproto import Message as CA_Message
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import AlarmSeverity
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
from streaming_data_types.logdata_f142 import serialise_f142

from forwarder.epics_to_serialisable_types import (
    ca_alarm_status_to_f142,
    epics_alarm_severity_to_f142,
    numpy_type_from_caproto_type,
    numpy_type_from_p4p_type,
    pva_alarm_message_to_f142_alarm_status,
)
from forwarder.kafka.kafka_helpers import seconds_to_nanoseconds
from forwarder.update_handlers.schema_serialisers import CASerialiser, PVASerialiser


def _get_alarm_status(response):
    try:
        alarm_status = pva_alarm_message_to_f142_alarm_status[response.alarm.message]
    except KeyError:
        alarm_status = AlarmStatus.UDF
    return alarm_status


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
    alarm: AlarmStatus,
    severity: AlarmSeverity,
    value: np.ndarray,
    timestamp: int,
) -> Tuple[bytes, int]:
    extra_arguments = {"alarm_status": alarm, "alarm_severity": severity}
    return (
        serialise_f142(value, source_name, timestamp, **extra_arguments),
        timestamp,
    )


# to-do: rename classes
class CA_f142_Serialiser(CASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name

    def serialise(
        self, update: CA_Message, **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        alarm = ca_alarm_status_to_f142[update.metadata.status]
        severity = epics_alarm_severity_to_f142[update.metadata.severity]
        timestamp = seconds_to_nanoseconds(update.metadata.timestamp)
        value = _extract_ca_data(update)
        return _serialise(self._source_name, alarm, severity, value, timestamp)

    def ca_conn_serialise(self, pv: str, state: str) -> Tuple[None, None]:
        return None, None


class PVA_f142_Serialiser(PVASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name

    def serialise(
        self, update: Union[p4p.Value, RuntimeError]
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        if isinstance(update, RuntimeError):
            return None, None
        alarm = _get_alarm_status(update)
        severity = epics_alarm_severity_to_f142[update.alarm.severity]
        value = _extract_pva_data(update)
        timestamp = (
            update.timeStamp.secondsPastEpoch * 1_000_000_000
        ) + update.timeStamp.nanoseconds
        return _serialise(self._source_name, alarm, severity, value, timestamp)
