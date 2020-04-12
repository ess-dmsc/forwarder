import numpy as np
from caproto import ChannelType
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import AlarmSeverity

# caproto can give us values of different dtypes even from the same EPICS channel,
# for example it will use the smallest integer type it can for the particular value,
# for example ">i2" (big-endian, 2 byte int).
# Unfortunately the serialisation method doesn't know what to do with such a specific dtype
# so we will cast to a consistent type based on the EPICS channel type.
numpy_type_from_channel_type = {
    ChannelType.CTRL_INT: np.int32,
    ChannelType.CTRL_LONG: np.int64,
    ChannelType.CTRL_FLOAT: np.float32,
    ChannelType.CTRL_DOUBLE: np.float64,
    ChannelType.CTRL_STRING: np.unicode_,
    ChannelType.CTRL_ENUM: np.int32,
    ChannelType.CTRL_CHAR: np.unicode_,
    ChannelType.TIME_STRING: np.unicode_,
}

caproto_alarm_severity_to_f142 = {
    0: AlarmSeverity.NO_ALARM,
    1: AlarmSeverity.MINOR,
    2: AlarmSeverity.MAJOR,
    3: AlarmSeverity.INVALID,
}

caproto_alarm_status_to_f142 = {
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
