import numpy as np
from caproto import ChannelType
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import AlarmSeverity

# caproto can give us values of different dtypes even from the same EPICS channel,
# for example it will use the smallest integer type it can for the particular value,
# for example ">i2" (big-endian, 2 byte int).
# Unfortunately the serialisation method doesn't know what to do with such a specific dtype
# so we will cast to a consistent type based on the EPICS channel type.
numpy_type_from_caproto_type = {
    ChannelType.TIME_INT: np.int32,
    ChannelType.TIME_LONG: np.int64,
    ChannelType.TIME_FLOAT: np.float32,
    ChannelType.TIME_DOUBLE: np.float64,
    ChannelType.TIME_STRING: np.unicode_,
    ChannelType.TIME_ENUM: np.int32,
    ChannelType.TIME_CHAR: np.unicode_,
}

numpy_type_from_p4p_type = {
    "d": np.float64,
    "f": np.float32,
    "L": np.uint64,
    "I": np.uint32,
    "i": np.int32,
    "l": np.int64,
    "H": np.uint16,
    "h": np.int16,
    "B": np.uint8,
    "b": np.int8,
    "s": np.unicode_,
    "?": np.bool8,
}

epics_alarm_severity_to_f142 = {
    0: AlarmSeverity.NO_ALARM,
    1: AlarmSeverity.MINOR,
    2: AlarmSeverity.MAJOR,
    3: AlarmSeverity.INVALID,
}

ca_alarm_status_to_f142 = {
    0: AlarmStatus.NO_ALARM,
    1: AlarmStatus.READ,
    2: AlarmStatus.WRITE,
    3: AlarmStatus.HIHI,
    4: AlarmStatus.HIGH,
    5: AlarmStatus.LOLO,
    6: AlarmStatus.LOW,
    7: AlarmStatus.STATE,
    8: AlarmStatus.COS,
    9: AlarmStatus.COMM,
    10: AlarmStatus.TIMED,
    11: AlarmStatus.HWLIMIT,
    12: AlarmStatus.CALC,
    13: AlarmStatus.SCAN,
    14: AlarmStatus.LINK,
    15: AlarmStatus.SOFT,
    16: AlarmStatus.BAD_SUB,
    17: AlarmStatus.UDF,
    18: AlarmStatus.DISABLE,
    19: AlarmStatus.SIMM,
    20: AlarmStatus.READ_ACCESS,
    21: AlarmStatus.WRITE_ACCESS,
}

# When using PVA we have to resort to mapping the alarm status message string to our enum
pva_alarm_message_to_f142_alarm_status = {
    "NO_ALARM": AlarmStatus.NO_ALARM,
    "READ_ALARM": AlarmStatus.READ,
    "WRITE_ALARM": AlarmStatus.WRITE,
    "HIHI_ALARM": AlarmStatus.HIHI,
    "HIGH_ALARM": AlarmStatus.HIGH,
    "LOLO_ALARM": AlarmStatus.LOLO,
    "LOW_ALARM": AlarmStatus.LOW,
    "STATE_ALARM": AlarmStatus.STATE,
    "COS_ALARM": AlarmStatus.COS,
    "COMM_ALARM": AlarmStatus.COMM,
    "TIMED_ALARM": AlarmStatus.TIMED,
    "HWLIMIT_ALARM": AlarmStatus.HWLIMIT,
    "CALC_ALARM": AlarmStatus.CALC,
    "SCAN_ALARM": AlarmStatus.SCAN,
    "LINK_ALARM": AlarmStatus.LINK,
    "SOFT_ALARM": AlarmStatus.SOFT,
    "BAD_SUB_ALARM": AlarmStatus.BAD_SUB,
    "UDF_ALARM": AlarmStatus.UDF,
    "DISABLE_ALARM": AlarmStatus.DISABLE,
    "SIMM_ALARM": AlarmStatus.SIMM,
    "READ_ACCESS_ALARM": AlarmStatus.READ_ACCESS,
    "WRITE_ACCESS_ALARM": AlarmStatus.WRITE_ACCESS,
}
