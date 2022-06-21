from typing import Tuple, Union, Optional, Dict

import time
import p4p
from caproto import Message as CA_Message
from streaming_data_types.epics_pv_conn_status_pvCn import serialise_pvCn
from streaming_data_types.fbschemas.epics_conn_status_pvCn.ConnectionInfo import (
    ConnectionInfo,
)
from p4p.client.thread import Cancelled, Disconnected, RemoteError, Finished

from forwarder.kafka.kafka_helpers import seconds_to_nanoseconds
from datetime import datetime, timezone


class pvCn_Serialiser:
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._conn_status: ConnectionInfo = ConnectionInfo.NEVER_CONNECTED

    def _serialise(self, timestamp_ns: int) -> Tuple[bytes, int]:
        return (
            serialise_pvCn(
                timestamp=datetime.fromtimestamp(timestamp_ns / 1e9, timezone.utc),
                status=self._conn_status,
                source_name=self._source_name,
            ),
            timestamp_ns,
        )

    def start_state_serialise(self):
        return self._serialise(seconds_to_nanoseconds(time.time()))

    def pva_serialise(
        self, update: Union[p4p.Value, RuntimeError], **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        if isinstance(update, p4p.Value):
            timestamp = (
                update.timeStamp.secondsPastEpoch * 1_000_000_000
                + update.timeStamp.nanoseconds
            )
            if self._conn_status == ConnectionInfo.CONNECTED:
                return None, None
            elif self._conn_status == ConnectionInfo.NEVER_CONNECTED:
                self._conn_status = ConnectionInfo.CONNECTED
                return self._serialise(timestamp)
        conn_state_map = {
            Cancelled: ConnectionInfo.CANCELLED,
            Disconnected: ConnectionInfo.DISCONNECTED,
            RemoteError: ConnectionInfo.REMOTE_ERROR,
            Finished: ConnectionInfo.DESTROYED,
        }
        self._conn_status = conn_state_map.get(type(update), ConnectionInfo.UNKNOWN)
        return self._serialise(seconds_to_nanoseconds(time.time()))

    def ca_serialise(
        self, update: CA_Message, **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        return None, None

    def ca_conn_serialise(
        self, pv: str, state: str
    ) -> Tuple[Optional[bytes], Optional[int]]:
        state_str_to_enum: Dict[str, ConnectionInfo] = {
            "connected": ConnectionInfo.CONNECTED,
            "disconnected": ConnectionInfo.DISCONNECTED,
            "destroyed": ConnectionInfo.DESTROYED,
        }
        self._conn_status = state_str_to_enum.get(state, ConnectionInfo.UNKNOWN)
        return self._serialise(seconds_to_nanoseconds(time.time()))
