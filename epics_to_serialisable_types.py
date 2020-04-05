import numpy as np
from caproto import ChannelType

# caproto can give us values of different dtypes even from the same EPICS channel,
# for example it will use the smallest integer type it can for the particular value,
# for example ">i2" (big-endian, 2 byte int).
# Unfortunately the serialisation method doesn't know what to do with such a specific dtype
# so we will cast to a consistent type based on the EPICS channel type.
numpy_type_from_channel_type = {
    ChannelType.CTRL_INT: np.int32,
    ChannelType.CTRL_LONG: np.int64,
    ChannelType.CTRL_FLOAT: np.float,
    ChannelType.CTRL_DOUBLE: np.float64,
    ChannelType.CTRL_STRING: np.unicode_,
}
