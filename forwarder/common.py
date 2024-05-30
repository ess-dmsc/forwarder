from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

from streaming_data_types.fbschemas.forwarder_config_update_fc00.Protocol import (
    Protocol,
)
from streaming_data_types.fbschemas.forwarder_config_update_fc00.UpdateType import (
    UpdateType,
)


class CommandType(Enum):
    ADD = "add"
    REMOVE = "stop_channel"
    REMOVE_ALL = "stop_all"
    REPLACE = "replace_all"
    INVALID = "invalid_config_update"


class EpicsProtocol(Enum):
    PVA = "pva"
    CA = "ca"
    FAKE = "fake"
    NONE = "none"


# Using frozen=True makes instances of Channel immutable
# and means it generates a __hash__ method so that we can use it as a dictionary key
@dataclass(frozen=True)
class Channel:
    name: Optional[str]
    protocol: EpicsProtocol
    output_topic: Optional[str]
    schema: Optional[str]
    periodic: Optional[int]


@dataclass(frozen=True)
class ConfigUpdate:
    command_type: CommandType
    channels: Optional[Tuple[Channel, ...]]


config_change_to_command_type = {
    UpdateType.ADD: CommandType.ADD,
    UpdateType.REMOVE: CommandType.REMOVE,
    UpdateType.REMOVEALL: CommandType.REMOVE_ALL,
    UpdateType.REPLACE: CommandType.REPLACE,
}

config_protocol_to_epics_protocol = {
    Protocol.PVA: EpicsProtocol.PVA,
    Protocol.CA: EpicsProtocol.CA,
    Protocol.FAKE: EpicsProtocol.FAKE,
}
