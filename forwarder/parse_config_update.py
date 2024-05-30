from typing import Generator, List

from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.forwarder_config_update_fc00 import (
    StreamInfo,
    deserialise_fc00,
)

from forwarder.application_logger import get_logger
from forwarder.common import (
    Channel,
    CommandType,
    ConfigUpdate,
    config_change_to_command_type,
    config_protocol_to_epics_protocol,
)
from forwarder.update_handlers.schema_serialiser_factory import SerialiserFactory

logger = get_logger()


def parse_config_update(config_update_payload: bytes) -> ConfigUpdate:
    try:
        config_update = deserialise_fc00(config_update_payload)
        command_type = config_change_to_command_type[config_update.config_change]
    except WrongSchemaException:
        logger.warning("Ignoring received message as it had the wrong schema")
        return ConfigUpdate(CommandType.INVALID, None)
    except KeyError:
        logger.warning(
            "Unrecognised configuration change type in configuration update message"
        )
        return ConfigUpdate(CommandType.INVALID, None)
    except Exception:
        logger.warning(
            "Unable to deserialise payload of received configuration update message"
        )
        return ConfigUpdate(CommandType.INVALID, None)

    if command_type == CommandType.REMOVE_ALL:
        return ConfigUpdate(CommandType.REMOVE_ALL, None)

    parsed_streams = tuple(_parse_streams(command_type, config_update.streams))
    if (
        command_type in [CommandType.ADD, CommandType.REMOVE, CommandType.REPLACE]
        and not parsed_streams
    ):
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

        try:
            epics_protocol = config_protocol_to_epics_protocol[stream.protocol]
        except KeyError:
            logger.warning(
                f'Unrecognised protocol type "{stream.protocol}" specified for'
                f"stream in configuration update message."
            )
            continue

        if epics_protocol not in SerialiserFactory.get_protocols():
            logger.warning(
                f'Serialiser for protocol "{stream.protocol}" ({epics_protocol})'
                f"not found for stream in configuration update message."
            )
            continue

        if stream.schema and stream.schema not in SerialiserFactory.get_schemas(
            epics_protocol
        ):
            logger.warning(
                f'Unsupported schema type "{stream.schema}" for protocol "{epics_protocol}"'
                f"specified for stream in configuration update message."
            )
            continue

        yield Channel(
            stream.channel, epics_protocol, stream.topic, stream.schema, stream.periodic
        )
