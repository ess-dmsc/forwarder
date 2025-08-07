from typing import Optional, Tuple, Union
import p4p
from caproto import Message as CA_Message

from forwarder.application_logger import get_logger
from streaming_data_types import serialise_un00

from forwarder.kafka.kafka_helpers import seconds_to_nanoseconds
from forwarder.update_handlers.schema_serialisers import CASerialiser, PVASerialiser

logger = get_logger()

def _serialise(
    source_name: str,
    timestamp_ns: int | None,
    units: str | None,
) -> Tuple[bytes, int]:
    return (
        serialise_un00(source_name, timestamp_ns, units),
        timestamp_ns,
    )


class un00_CASerialiser(CASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._message: Optional[str] = None
        self._units: Optional[str] = None

    def serialise(
        self, update: CA_Message, **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        metadata = update.metadata
        try:
            timestamp = seconds_to_nanoseconds(metadata.timestamp)
        except AttributeError:
            logger.warning("No timestamp available for %s", self._source_name)
            timestamp = None
        try:
            units = metadata.units
        except AttributeError:
            logger.warning("No units available for %s", self._source_name)
            return [None, None]
        logger.debug(f"Source name: {self._source_name}, timestamp: {timestamp}, units: {units}")
        return _serialise(self._source_name, timestamp, units)

    def conn_serialise(self, pv: str, state: str) -> Tuple[None, None]:
        return None, None


class un00_PVASerialiser(PVASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._message: Optional[str] = None
        self._units: Optional[str] = None

    def serialise(
        self, update: Union[p4p.Value, RuntimeError]
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        if isinstance(update, RuntimeError):
            return None, None
        timestamp = (
            update.timeStamp.secondsPastEpoch * 1_000_000_000
        ) + update.timeStamp.nanoseconds

        try:
            self._units = update.display.units
        except AttributeError:
            logger.warning("No units available for %s", self._source_name)
            self._units = None

        return _serialise(self._source_name, timestamp, self._units)
