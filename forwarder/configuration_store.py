import time
from typing import Dict
from unittest import mock

from confluent_kafka import TopicPartition
from streaming_data_types.fbschemas.forwarder_config_update_fc00.UpdateType import (
    UpdateType,
)
from streaming_data_types.forwarder_config_update_fc00 import (
    Protocol,
    StreamInfo,
    deserialise_fc00,
    serialise_fc00,
)

from forwarder.common import EpicsProtocol


class ConfigurationStore:
    def __init__(self, producer, consumer, topic):
        self._producer = producer
        self._consumer = consumer
        self._topic = topic

    def save_configuration(self, update_handlers: Dict):
        streams = []
        for channel, update_handler in update_handlers.items():
            protocol_map = {
                EpicsProtocol.CA: Protocol.Protocol.CA,
                EpicsProtocol.FAKE: Protocol.Protocol.FAKE,
                EpicsProtocol.PVA: Protocol.Protocol.PVA,
            }
            stream = StreamInfo(
                channel.name,
                channel.schema,
                channel.output_topic,
                protocol_map[channel.protocol],
                channel.periodic,
            )

            streams.append(stream)
        if streams:
            message = serialise_fc00(UpdateType.ADD, streams)
        else:
            # No streams so store a "blank" config
            message = serialise_fc00(UpdateType.REMOVEALL, streams)
        self._producer.produce(self._topic, bytes(message), int(time.time() * 1000))

    def retrieve_configuration(self):
        """Retrieve last valid configuration buffer."""
        topic = TopicPartition(self._topic, partition=0)
        low_offset, high_offset = self._consumer.get_watermark_offsets(topic)
        # Set offset to current_offset to start retrieving from last message
        current_offset = high_offset - 1
        topic.offset = current_offset
        self._consumer.assign([topic])

        while current_offset >= low_offset:
            msg = self._consumer.consume(timeout=2)
            if msg and self._is_a_valid_configuration_buffer(msg[-1].value()):
                return msg[-1].value()
            current_offset -= 1
            topic.offset = current_offset
            self._consumer.seek(topic)

        raise RuntimeError("Could not retrieve stored configuration")

    def stop(self):
        self._producer.close()
        self._consumer.close()

    @staticmethod
    def _is_a_valid_configuration_buffer(payload):
        try:
            _ = deserialise_fc00(payload)
            return True
        except Exception:
            return False


NullConfigurationStore = mock.create_autospec(ConfigurationStore)
