import numpy as np
from caproto import ChannelType
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import (
    AlarmSeverity as f142_AlarmSeverity,
)
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import (
    AlarmStatus as f142_AlarmStatus,
)

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
    "?": np.bool_,
}

epics_alarm_severity_to_f142 = {
    0: f142_AlarmSeverity.NO_ALARM,
    1: f142_AlarmSeverity.MINOR,
    2: f142_AlarmSeverity.MAJOR,
    3: f142_AlarmSeverity.INVALID,
}

ca_alarm_status_to_f142 = {
    0: f142_AlarmStatus.NO_ALARM,
    1: f142_AlarmStatus.READ,
    2: f142_AlarmStatus.WRITE,
    3: f142_AlarmStatus.HIHI,
    4: f142_AlarmStatus.HIGH,
    5: f142_AlarmStatus.LOLO,
    6: f142_AlarmStatus.LOW,
    7: f142_AlarmStatus.STATE,
    8: f142_AlarmStatus.COS,
    9: f142_AlarmStatus.COMM,
    10: f142_AlarmStatus.TIMED,
    11: f142_AlarmStatus.HWLIMIT,
    12: f142_AlarmStatus.CALC,
    13: f142_AlarmStatus.SCAN,
    14: f142_AlarmStatus.LINK,
    15: f142_AlarmStatus.SOFT,
    16: f142_AlarmStatus.BAD_SUB,
    17: f142_AlarmStatus.UDF,
    18: f142_AlarmStatus.DISABLE,
    19: f142_AlarmStatus.SIMM,
    20: f142_AlarmStatus.READ_ACCESS,
    21: f142_AlarmStatus.WRITE_ACCESS,
}

# When using PVA we have to resort to mapping the alarm status message string to our enum
pva_alarm_message_to_f142_alarm_status = {
    "NO_ALARM": f142_AlarmStatus.NO_ALARM,
    "READ_ALARM": f142_AlarmStatus.READ,
    "WRITE_ALARM": f142_AlarmStatus.WRITE,
    "HIHI_ALARM": f142_AlarmStatus.HIHI,
    "HIGH_ALARM": f142_AlarmStatus.HIGH,
    "LOLO_ALARM": f142_AlarmStatus.LOLO,
    "LOW_ALARM": f142_AlarmStatus.LOW,
    "STATE_ALARM": f142_AlarmStatus.STATE,
    "COS_ALARM": f142_AlarmStatus.COS,
    "COMM_ALARM": f142_AlarmStatus.COMM,
    "TIMED_ALARM": f142_AlarmStatus.TIMED,
    "HWLIMIT_ALARM": f142_AlarmStatus.HWLIMIT,
    "CALC_ALARM": f142_AlarmStatus.CALC,
    "SCAN_ALARM": f142_AlarmStatus.SCAN,
    "LINK_ALARM": f142_AlarmStatus.LINK,
    "SOFT_ALARM": f142_AlarmStatus.SOFT,
    "BAD_SUB_ALARM": f142_AlarmStatus.BAD_SUB,
    "UDF_ALARM": f142_AlarmStatus.UDF,
    "DISABLE_ALARM": f142_AlarmStatus.DISABLE,
    "SIMM_ALARM": f142_AlarmStatus.SIMM,
    "READ_ACCESS_ALARM": f142_AlarmStatus.READ_ACCESS,
    "WRITE_ACCESS_ALARM": f142_AlarmStatus.WRITE_ACCESS,
}
