import json
from forwarder.application_logger import get_logger
import attr
from enum import Enum
from typing import Tuple, Generator, Dict, Optional
from forwarder.kafka.kafka_helpers import get_broker_and_topic_from_uri

logger = get_logger()


class CommandType(Enum):
    ADD = "add"
    REMOVE = "stop_channel"
    REMOVE_ALL = "stop_all"
    EXIT = "exit"


class EpicsProtocol(Enum):
    PVA = "pva"
    CA = "ca"
    FAKE = "fake"
    NONE = "none"


@attr.s
class Channel:
    name = attr.ib(type=str)
    protocol = attr.ib(type=EpicsProtocol)
    output_topic = attr.ib(type=str)
    schema = attr.ib(type=str)


@attr.s
class ConfigUpdate:
    command_type = attr.ib(type=CommandType)
    channels = attr.ib(type=Optional[Tuple[Channel, ...]])


def parse_config_update(config_update_payload: str) -> Optional[ConfigUpdate]:
    config = json.loads(config_update_payload)
    try:
        command_type = CommandType(config["cmd"])
    except KeyError:
        logger.warning('Message received in config topic contained no "cmd" field')
        return None
    except ValueError:
        logger.warning(f'Unrecognised command "{config["cmd"]}" received')
        return None

    if command_type == CommandType.REMOVE_ALL or command_type == CommandType.EXIT:
        return ConfigUpdate(command_type, None)

    if command_type == CommandType.REMOVE:
        try:
            channel_name = config["channel"]
        except ValueError:
            logger.warning(
                f'"channel" field not found in received "{command_type}" command'
            )
            return None
        return ConfigUpdate(
            command_type, (Channel(channel_name, EpicsProtocol.NONE, "", ""),)
        )

    try:
        streams = config["streams"]
    except KeyError:
        logger.warning('Message received in config topic contained no "streams" field')
        return None

    return ConfigUpdate(command_type, tuple(_parse_streams(command_type, streams)))


def _parse_streams(
    command_type: CommandType, streams: Dict
) -> Generator[Channel, None, None]:
    for update_stream in streams:
        try:
            channel = update_stream["channel"]
        except ValueError:
            logger.warning(
                f'"channel" field not found in "stream" entry in received "{command_type}" command'
            )
            continue

        if "channel_provider_type" in update_stream.keys():
            try:
                protocol = EpicsProtocol(update_stream["channel_provider_type"])
            except ValueError:
                logger.warning(
                    f'Unrecognised "channel_provider_type" {update_stream["channel_provider_type"]} '
                    f"provided in configuration change"
                )
                continue
        else:
            protocol = EpicsProtocol.PVA

        try:
            _, output_topic = get_broker_and_topic_from_uri(
                update_stream["converter"]["topic"], broker_required=False
            )
        except ValueError:
            logger.warning(
                f'"topic" field not found in "stream" entry in received "{command_type}" command'
            )
            continue
        except RuntimeError as e:
            logger.warning(e)
            continue

        try:
            schema = update_stream["converter"]["schema"]
        except ValueError:
            logger.warning(
                f'"schema" field not found in "stream" entry in received "{command_type}" command'
            )
            continue

        yield Channel(channel, protocol, output_topic, schema)
