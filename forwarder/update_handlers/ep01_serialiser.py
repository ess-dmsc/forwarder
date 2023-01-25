import time
from typing import Dict, Optional, Tuple, Union

import p4p
from caproto import Message as CA_Message
from p4p.client.thread import Cancelled, Disconnected, Finished, RemoteError
from streaming_data_types.epics_connection_ep01 import ConnectionInfo, serialise_ep01

from forwarder.kafka.kafka_helpers import seconds_to_nanoseconds
from forwarder.update_handlers.schema_serialisers import CASerialiser, PVASerialiser


def _serialise(
    source_name: str, conn_status: ConnectionInfo, timestamp_ns: int
) -> Tuple[bytes, int]:
    return (
        serialise_ep01(
            timestamp_ns=timestamp_ns,
            status=conn_status,
            source_name=source_name,
        ),
        timestamp_ns,
    )


class ep01_CASerialiser(CASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._conn_status: ConnectionInfo = ConnectionInfo.NEVER_CONNECTED
        self._state_str_to_enum: Dict[str, ConnectionInfo] = {
            "connected": ConnectionInfo.CONNECTED,
            "disconnected": ConnectionInfo.DISCONNECTED,
            "destroyed": ConnectionInfo.DESTROYED,
            "cancelled": ConnectionInfo.CANCELLED,
            "finished": ConnectionInfo.FINISHED,
            "remote_error": ConnectionInfo.REMOTE_ERROR,
        }

    def serialise(
        self, update: CA_Message, **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        return None, None

    def conn_serialise(
        self, pv: str, state: str
    ) -> Tuple[Optional[bytes], Optional[int]]:
        self._conn_status = self._state_str_to_enum.get(state, ConnectionInfo.UNKNOWN)
        return _serialise(
            self._source_name, self._conn_status, seconds_to_nanoseconds(time.time())
        )

    def start_state_serialise(self):
        from forwarder.kafka.kafka_helpers import seconds_to_nanoseconds

        return _serialise(
            self._source_name, self._conn_status, seconds_to_nanoseconds(time.time())
        )


class ep01_PVASerialiser(PVASerialiser):
    def __init__(self, source_name: str):
        self._source_name = source_name
        self._conn_status: ConnectionInfo = ConnectionInfo.NEVER_CONNECTED
        self._conn_state_map = {
            p4p.Value: ConnectionInfo.CONNECTED,
            Cancelled: ConnectionInfo.CANCELLED,
            Disconnected: ConnectionInfo.DISCONNECTED,
            RemoteError: ConnectionInfo.REMOTE_ERROR,
            Finished: ConnectionInfo.FINISHED,
        }

    def serialise(
        self, update: Union[p4p.Value, RuntimeError], **unused
    ) -> Union[Tuple[bytes, int], Tuple[None, None]]:
        timestamp = seconds_to_nanoseconds(time.time())

        if isinstance(update, p4p.Value):
            timestamp = (
                update.timeStamp.secondsPastEpoch * 1_000_000_000
                + update.timeStamp.nanoseconds
            )

        conn_status = self.conn_state_map.get(type(update), ConnectionInfo.UNKNOWN)

        if conn_status == self._conn_status:
            # Nothing has changed
            return None, None

        if (
            self._conn_status == ConnectionInfo.NEVER_CONNECTED
            and conn_status != ConnectionInfo.CONNECTED
        ):
            return None, None

        self._conn_status = conn_status
        return _serialise(self._source_name, self._conn_status, timestamp)

    def start_state_serialise(self):
        from forwarder.kafka.kafka_helpers import seconds_to_nanoseconds

        return _serialise(
            self._source_name, self._conn_status, seconds_to_nanoseconds(time.time())
        )
