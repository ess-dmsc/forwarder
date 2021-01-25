from forwarder.application_logger import get_logger
import attr
from enum import Enum
from typing import Tuple, Generator, Optional, List
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.forwarder_config_update_rf5k import (
    deserialise_rf5k,
    StreamInfo,
)
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.Protocol import (
    Protocol,
)
from forwarder.update_handlers.schema_publishers import schema_publishers
from flatbuffers.packer import struct as flatbuffer_struct

logger = get_logger()


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


def parse_config_update(config_update_payload: bytes) -> ConfigUpdate:
    try:
        config_update = deserialise_rf5k(config_update_payload)
    except (RuntimeError, flatbuffer_struct.error):
        logger.warning(
            "Unable to deserialise payload of received configuration update message"
        )
        return ConfigUpdate(CommandType.INVALID, None)
    except WrongSchemaException:
        logger.warning("Ignoring received message as it had the wrong schema")
        return ConfigUpdate(CommandType.INVALID, None)

    try:
        command_type = config_change_to_command_type[config_update.config_change]
    except KeyError:
        logger.warning(
            "Unrecogised configuration change type in configuration update message"
        )
        return ConfigUpdate(CommandType.INVALID, None)

    if command_type == CommandType.REMOVE_ALL:
        return ConfigUpdate(CommandType.REMOVE_ALL, None)

    parsed_streams = tuple(_parse_streams(command_type, config_update.streams))
    if (
        command_type == CommandType.ADD or command_type == CommandType.REMOVE
    ) and not parsed_streams:
        logger.warning(
            "Configuration update message requests adding or removing streams "
            "but does not contain valid details of streams"
        )
        return ConfigUpdate(CommandType.INVALID, None)

    return ConfigUpdate(command_type, parsed_streams)


def _parse_streams(
    command_type: CommandType, streams: List[StreamInfo]
) -> Generator[Channel, None, None]:
    for stream in streams:
        fields_present = (bool(stream.channel), bool(stream.schema), bool(stream.topic))
        if command_type == CommandType.ADD and not all(fields_present):
            logger.warning(
                f"All details must be given when adding a stream, but received ADD request for "
                f"channel='{stream.channel}', schema='{stream.schema}', topic='{stream.topic}'. Skipping."
            )
            continue

        if command_type == CommandType.REMOVE and not any(fields_present):
            logger.warning(
                f"At least one of channel, schema or topic must be given when removing a stream, but received REMOVE "
                f"request for channel='{stream.channel}', schema='{stream.schema}', topic='{stream.topic}'. Skipping."
            )
            continue

        if stream.schema and stream.schema not in schema_publishers.keys():
            logger.warning(
                f'Unsupported schema type "{stream.schema}" specified for'
                f"stream in configuration update message."
            )
            continue

        try:
            epics_protocol = config_protocol_to_epics_protocol[stream.protocol]
        except KeyError:
            logger.warning(
                f'Unrecognised protocol type "{stream.protocol}" specified for'
                f"stream in configuration update message."
            )
            continue

        yield Channel(stream.channel, epics_protocol, stream.topic, stream.schema)
