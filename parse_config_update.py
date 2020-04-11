import json
from application_logger import get_logger
import attr
from enum import Enum
from typing import Tuple, Union, Generator, Dict, Optional

logger = get_logger()


class CommandType(Enum):
    ADD = "add"
    REMOVE = "stop_channel"
    REMOVE_ALL = "stop_all"
    EXIT = "exit"


class EpicsProtocol(Enum):
    PVA = "pva"
    CA = "ca"


@attr.s
class Channel:
    name = attr.ib(type=str)
    protocol = attr.ib(type=Optional[EpicsProtocol])


@attr.s
class ConfigUpdate:
    command_type = attr.ib(type=CommandType)
    channels = attr.ib(type=Optional[Tuple[Channel]])


def parse_config_update(config_update_payload: str) -> Union[ConfigUpdate, None]:
    config = json.loads(config_update_payload)
    try:
        command_type = CommandType(config["cmd"])
    except KeyError:
        logger.warning('Message received in config topic contained no "cmd" field')
        return
    except ValueError:
        logger.warning(f'Unrecognised command "{config["cmd"]}" received')
        return

    if command_type == CommandType.REMOVE_ALL or command_type == CommandType.EXIT:
        return ConfigUpdate(command_type, None)

    if command_type == CommandType.REMOVE:
        try:
            channel_name = config["channel"]
        except ValueError:
            logger.warning(
                f'"channel" field not found in received "{command_type}" command'
            )
            return
        return ConfigUpdate(command_type, (Channel(channel_name, None),))

    try:
        streams = config["streams"]
    except KeyError:
        logger.warning('Message received in config topic contained no "streams" field')
        return

    return ConfigUpdate(command_type, tuple(_parse_streams(command_type, streams)))


def _parse_streams(command_type: CommandType, streams: Dict) -> Generator[Channel]:
    for update_stream in streams:
        if "channel" not in update_stream.keys():
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

        yield Channel(update_stream["channel"], protocol)
