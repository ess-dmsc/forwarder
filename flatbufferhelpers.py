import flatbuffers
from f142_logdata import LogData
from f142_logdata.Value import Value
from f142_logdata.Int import IntStart, IntAddValue, IntEnd


def _millseconds_to_nanoseconds(time_ms):
    return int(time_ms * 1000000)


def create_f142_message(data, timestamp_unix_ms=0):
    file_identifier = b"f142"
    builder = flatbuffers.Builder(1024)
    source = builder.CreateString("Forwarder-Python")
    IntStart(builder)
    IntAddValue(builder, int(data[0]))
    int_position = IntEnd(builder)

    # Build the actual buffer
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, int_position)
    LogData.LogDataAddValueType(builder, Value.Int)
    # LogData.LogDataAddTimestamp(builder, _millseconds_to_nanoseconds(timestamp_unix_ms))
    log_msg = LogData.LogDataEnd(builder)
    builder.Finish(log_msg)

    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = file_identifier
    return bytes(buff)
