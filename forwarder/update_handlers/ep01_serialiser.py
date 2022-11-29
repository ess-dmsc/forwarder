import time
from typing import Dict, Optional, Tuple, Union

import p4p
from caproto import Message as CA_Message
from p4p.client.thread import Cancelled, Disconnected, Finished, RemoteError
from streaming_data_types.epics_connection_ep01 import serialise_ep01
from streaming_data_types.fbschemas.epics_connection_ep01.EventType import EventType

from forwarder.kafka.kafka_helpers import seconds_to_nanoseconds
from forwarder.update_handlers.schema_serialisers import CASerialiser, PVASerialiser


def _serialise(
    source_name: str, conn_status: EventType, timestamp_ns: int
) -> Tuple[bytes, int]:
    return (
        serialise_ep01(
            timestamp_ns=timestamp_ns,
            event_type=conn_status,
            source_name=source_name,
        ),
        timestamp_ns,
    )


class ep01_CASerialiser(CASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._conn_status: EventType = EventType.NEVER_CONNECTED

    def serialise(
        self, update: CA_Message, **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        return None, None

    def conn_serialise(
        self, pv: str, state: str
    ) -> Tuple[Optional[bytes], Optional[int]]:
        state_str_to_enum: Dict[str, EventType] = {
            "connected": EventType.CONNECTED,
            "disconnected": EventType.DISCONNECTED,
            "destroyed": EventType.DESTROYED,
        }
        self._conn_status = state_str_to_enum.get(state, EventType.UNKNOWN)
        return _serialise(
            self._source_name, self._conn_status, seconds_to_nanoseconds(time.time())
        )

    def start_state_serialise(self):
        return _serialise(
            self._source_name, self._conn_status, seconds_to_nanoseconds(time.time())
        )


class ep01_PVASerialiser(PVASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._conn_status: EventType = EventType.NEVER_CONNECTED

    def serialise(
        self, update: Union[p4p.Value, RuntimeError], **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        if isinstance(update, p4p.Value):
            timestamp = (
                update.timeStamp.secondsPastEpoch * 1_000_000_000
                + update.timeStamp.nanoseconds
            )
            if self._conn_status == EventType.CONNECTED:
                return None, None
            elif self._conn_status == EventType.NEVER_CONNECTED:
                self._conn_status = EventType.CONNECTED
                return _serialise(self._source_name, self._conn_status, timestamp)
        conn_state_map = {
            Cancelled: EventType.DESTROYED,
            Disconnected: EventType.DISCONNECTED,
            RemoteError: EventType.DISCONNECTED,
            Finished: EventType.DESTROYED,
        }
        self._conn_status = conn_state_map.get(type(update), EventType.UNKNOWN)
        return _serialise(
            self._source_name, self._conn_status, seconds_to_nanoseconds(time.time())
        )

    def start_state_serialise(self):
        return _serialise(
            self._source_name, self._conn_status, seconds_to_nanoseconds(time.time())
        )
