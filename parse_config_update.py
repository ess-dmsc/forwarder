import json
from application_logger import get_logger
import attr
from enum import Enum
from typing import Tuple, Union

logger = get_logger()


class CommandTypes(Enum):
    ADD = "add"
    REMOVE = "remove"


@attr.s
class ConfigUpdate:
    command_type = attr.ib(type=CommandTypes)
    channel_names = attr.ib(type=Tuple[str])


def parse_config_update(config_update_payload: str) -> Union[ConfigUpdate, None]:
    config = json.loads(config_update_payload)
    try:
        command_type = config["cmd"]
    except KeyError:
        logger.warning('Message received in config topic contained no "cmd" field')
        return

    if command_type not in set(command.value for command in CommandTypes):
        logger.warning(f'Unrecognised command "{command_type}" received')
        return

    try:
        streams = config["streams"]
    except KeyError:
        logger.warning('Message received in config topic contained no "streams" field')
        return

    channels = tuple(
        [
            str(update["channel"])
            if "channel" in update.keys()
            else logger.warning(
                f'"channel" field not found in "stream" entry in received "{command_type}" command'
            )
            for update in streams
        ]
    )

    return ConfigUpdate(command_type, channels)
