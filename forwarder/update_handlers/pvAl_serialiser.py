from typing import Tuple, Union, Dict
import p4p
from caproto import Message as CA_Message
from streaming_data_types.fbschemas.pv_alarm_state_pvAl.AlarmState import AlarmState
from streaming_data_types.fbschemas.pv_alarm_state_pvAl.CAAlarmState import CAAlarmState
from streaming_data_types.fbschemas.pv_alarm_state_pvAl.AlarmSeverity import (
    AlarmSeverity,
)
from streaming_data_types.epics_pv_alarm_status_pvAl import serialise_pvAl

from datetime import datetime, timedelta, timezone


# See the following link for the origin of this map:
# https://github.com/epics-base/pvAccessCPP/blob/7240f97e76c191f2c197d8d2986fa2355c88c801/src/ca/dbdToPv.cpp#L109
ca_alarm_status_to_alarm_state: Dict[CAAlarmState, AlarmState] = {
    CAAlarmState.NO_ALARM: AlarmState.NONE,
    CAAlarmState.READ: AlarmState.DEVICE,
    CAAlarmState.WRITE: AlarmState.DEVICE,
    CAAlarmState.HIHI: AlarmState.DEVICE,
    CAAlarmState.HIGH: AlarmState.DEVICE,
    CAAlarmState.LOLO: AlarmState.DEVICE,
    CAAlarmState.LOW: AlarmState.DEVICE,
    CAAlarmState.STATE: AlarmState.DEVICE,
    CAAlarmState.COS: AlarmState.DEVICE,
    CAAlarmState.HWLIMIT: AlarmState.DEVICE,
    CAAlarmState.COMM: AlarmState.DRIVER,
    CAAlarmState.TIMED: AlarmState.DRIVER,
    CAAlarmState.CALC: AlarmState.RECORD,
    CAAlarmState.SCAN: AlarmState.RECORD,
    CAAlarmState.LINK: AlarmState.RECORD,
    CAAlarmState.SOFT: AlarmState.RECORD,
    CAAlarmState.BAD_SUB: AlarmState.RECORD,
    CAAlarmState.DISABLE: AlarmState.DB,
    CAAlarmState.SIMM: AlarmState.DB,
    CAAlarmState.READ_ACCESS: AlarmState.DB,
    CAAlarmState.WRITE_ACCESS: AlarmState.DB,
    CAAlarmState.UDF: AlarmState.UNDEFINED,
}

ca_alarm_status_to_pvAl: Dict[int, CAAlarmState] = {
    0: CAAlarmState.NO_ALARM,
    1: CAAlarmState.READ,
    2: CAAlarmState.WRITE,
    3: CAAlarmState.HIHI,
    4: CAAlarmState.HIGH,
    5: CAAlarmState.LOLO,
    6: CAAlarmState.LOW,
    7: CAAlarmState.STATE,
    8: CAAlarmState.COS,
    9: CAAlarmState.COMM,
    10: CAAlarmState.TIMED,
    11: CAAlarmState.HWLIMIT,
    12: CAAlarmState.CALC,
    13: CAAlarmState.SCAN,
    14: CAAlarmState.LINK,
    15: CAAlarmState.SOFT,
    16: CAAlarmState.BAD_SUB,
    17: CAAlarmState.UDF,
    18: CAAlarmState.DISABLE,
    19: CAAlarmState.SIMM,
    20: CAAlarmState.READ_ACCESS,
    21: CAAlarmState.WRITE_ACCESS,
}

epics_alarm_severity_to_pvAl: Dict[int, AlarmSeverity] = {
    0: AlarmSeverity.NO_ALARM,
    1: AlarmSeverity.MINOR,
    2: AlarmSeverity.MAJOR,
    3: AlarmSeverity.INVALID,
    4: AlarmSeverity.UNDEFINED,
}

epics_alarm_state_to_pvAl: Dict[int, AlarmState] = {
    0: AlarmState.NONE,
    1: AlarmState.DEVICE,
    2: AlarmState.DRIVER,
    3: AlarmState.RECORD,
    4: AlarmState.DB,
    5: AlarmState.CONF,
    6: AlarmState.UNDEFINED,
    7: AlarmState.CLIENT,
}


def _extract_alarm_info(update: p4p.Value) -> Tuple[AlarmState, AlarmSeverity, str]:
    try:
        try:
            alarm = epics_alarm_state_to_pvAl[update.alarm.status]
            severity = epics_alarm_severity_to_pvAl[update.alarm.severity]
            msg = update.alarm.message
            return alarm, severity, msg
        except KeyError:
            return (
                AlarmState.UNDEFINED,
                AlarmSeverity.UNDEFINED,
                "Unknown alarm and/or severity.",
            )
    except AttributeError:
        return (
            AlarmState.UNDEFINED,
            AlarmSeverity.UNDEFINED,
            "No alarm (or missing) info in PV update",
        )


class pvAl_Serialiser:
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._current_alarm: Union[AlarmState] = None
        self._current_severity: Union[AlarmSeverity] = None

    def _serialise(
        self,
        timestamp,
        alarm: AlarmState,
        severity: AlarmSeverity,
        ca_alarm: CAAlarmState = CAAlarmState.UDF,
        message="",
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        if self._current_alarm == alarm and self._current_severity == severity:
            return None, None
        self._current_severity = severity
        self._current_alarm = alarm
        return (
            serialise_pvAl(
                source_name=self._source_name,
                timestamp=timestamp,
                state=alarm,
                ca_state=ca_alarm,
                severity=severity,
                message=message,
            ),
            timestamp.timestamp() / 1_000_000_000,
        )

    def _serialise_undefined(self) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        return self._serialise(
            datetime.now(tz=timezone.utc),
            alarm=AlarmState.UNDEFINED,
            ca_alarm=CAAlarmState.UDF,
            severity=AlarmSeverity.UNDEFINED,
        )

    def pva_serialise(
        self, update: Union[p4p.Value, RuntimeError]
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        if isinstance(update, RuntimeError):
            return self._serialise_undefined()
        alarm, severity, msg = _extract_alarm_info(update)

        timestamp = datetime.fromtimestamp(
            update.timeStamp.secondsPastEpoch, tz=timezone.utc
        ) + timedelta(microseconds=update.timeStamp.nanoseconds / 1000)
        return self._serialise(
            alarm=alarm,
            severity=severity,
            timestamp=timestamp,
            ca_alarm=CAAlarmState.UDF,
            message=msg,
        )

    def ca_serialise(
        self, update: CA_Message, **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        timestamp = datetime.fromtimestamp(update.metadata.timestamp, tz=timezone.utc)
        try:
            ca_alarm = ca_alarm_status_to_pvAl[update.metadata.status]
            alarm = ca_alarm_status_to_alarm_state[ca_alarm]
            severity = epics_alarm_severity_to_pvAl[update.metadata.severity]
        except KeyError:
            return self._serialise(
                alarm=AlarmState.UNDEFINED,
                severity=AlarmSeverity.UNDEFINED,
                ca_alarm=CAAlarmState.UDF,
                timestamp=timestamp,
            )
        return self._serialise(
            alarm=alarm, severity=severity, ca_alarm=ca_alarm, timestamp=timestamp
        )

    def ca_conn_serialise(
        self, pv: str, state: str
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        return self._serialise_undefined()
