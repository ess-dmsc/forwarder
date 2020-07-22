from streaming_data_types.forwarder_config_update_rf5k import serialise_rf5k, StreamInfo
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.Protocol import (
    Protocol,
)
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)
from typing import List


class ForwarderConfig:
    """
    Class that converts the pv information to a forwarder config message payload
    """

    def __init__(
        self, topic: str, epics_protocol: Protocol = Protocol.CA, schema: str = "f142"
    ):
        self.schema = schema
        self.topic = topic
        self.epics_protocol = epics_protocol

    def _create_streams(self, pvs: List[str]) -> List[StreamInfo]:
        return [
            StreamInfo(pv, self.schema, self.topic, self.epics_protocol) for pv in pvs
        ]

    def create_forwarder_configuration(self, pvs: List[str]) -> bytes:
        return serialise_rf5k(UpdateType.ADD, self._create_streams(pvs))

    def remove_forwarder_configuration(self, pvs: List[str]) -> bytes:
        return serialise_rf5k(UpdateType.REMOVE, self._create_streams(pvs))

    @staticmethod
    def remove_all_forwarder_configuration() -> bytes:
        return serialise_rf5k(UpdateType.REMOVEALL, [])
