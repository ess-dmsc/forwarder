from streaming_data_types.logdata_f142 import serialise_f142
import numpy as np
from typing import Union, Tuple
import p4p
from caproto import ReadNotifyResponse
from forwarder.epics_to_serialisable_types import (
    ca_alarm_status_to_f142,
    epics_alarm_severity_to_f142,
    numpy_type_from_caproto_type,
    numpy_type_from_p4p_type,
    pva_alarm_message_to_f142_alarm_status,
)
from forwarder.kafka.kafka_helpers import seconds_to_nanoseconds
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus


def _get_alarm_status(response):
    try:
        alarm_status = pva_alarm_message_to_f142_alarm_status[response.alarm.message]
    except KeyError:
        alarm_status = AlarmStatus.UDF
    return alarm_status


def _extract_pva_data(update: p4p.Value):
    if update.getID() == "epics:nt/NTEnum:1.0":
        return update.value.index
    data_type = numpy_type_from_p4p_type[update.type()["value"][-1]]
    return np.squeeze(np.array(update.value)).astype(data_type)


def _extract_ca_data(update: ReadNotifyResponse):
    data_type = numpy_type_from_caproto_type[update.data_type]
    return np.squeeze(np.array(update.data)).astype(data_type)


class f142_Serialiser:
    def __init__(self, source_name: str):
        self._source_name = source_name

    def serialise(
        self, update: Union[p4p.Value, ReadNotifyResponse], serialise_alarm: bool = True
    ) -> Tuple[bytes, int]:
        if isinstance(update, p4p.Value):
            alarm = _get_alarm_status(update)
            severity = epics_alarm_severity_to_f142[update.alarm.severity]
            value = _extract_pva_data(update)
            timestamp = (
                update.timeStamp.secondsPastEpoch * 1_000_000_000
            ) + update.timeStamp.nanoseconds
        else:
            alarm = ca_alarm_status_to_f142[update.metadata.status]
            severity = epics_alarm_severity_to_f142[update.metadata.severity]
            timestamp = seconds_to_nanoseconds(update.metadata.timestamp)
            value = _extract_ca_data(update)
        extra_arguments = {}
        if serialise_alarm:
            extra_arguments = {"alarm_status": alarm, "alarm_severity": severity}
        return (
            serialise_f142(value, self._source_name, timestamp, **extra_arguments),
            timestamp,
        )
