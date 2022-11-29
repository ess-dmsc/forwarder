from enum import Enum
from typing import Optional, Tuple

import attr
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.Protocol import (
    Protocol,
)
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)


class CommandType(Enum):
    ADD = "add"
    REMOVE = "stop_channel"
    REMOVE_ALL = "stop_all"
    INVALID = "invalid_config_update"


class EpicsProtocol(Enum):
    PVA = "pva"
    CA = "ca"
    FAKE = "fake"
    NONE = "none"


# Using frozen=True makes instances of Channel immutable
# and means attrs generates a __hash__ method so that we can use it as a dictionary key
@attr.s(frozen=True)
class Channel:
    name = attr.ib(type=Optional[str])
    protocol = attr.ib(type=EpicsProtocol)
    output_topic = attr.ib(type=Optional[str])
    schema = attr.ib(type=Optional[str])


@attr.s(frozen=True)
class ConfigUpdate:
    command_type = attr.ib(type=CommandType)
    channels = attr.ib(type=Optional[Tuple[Channel, ...]])


config_change_to_command_type = {
    UpdateType.ADD: CommandType.ADD,
    UpdateType.REMOVE: CommandType.REMOVE,
    UpdateType.REMOVEALL: CommandType.REMOVE_ALL,
}

config_protocol_to_epics_protocol = {
    Protocol.PVA: EpicsProtocol.PVA,
    Protocol.CA: EpicsProtocol.CA,
    Protocol.FAKE: EpicsProtocol.FAKE,
}
