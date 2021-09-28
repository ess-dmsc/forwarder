import time
from typing import Dict
from unittest import mock

from confluent_kafka import TopicPartition
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)
from streaming_data_types.forwarder_config_update_rf5k import (
    Protocol,
    StreamInfo,
    deserialise_rf5k,
    serialise_rf5k,
)

from forwarder.parse_config_update import EpicsProtocol


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
            )

            streams.append(stream)
        if streams:
            message = serialise_rf5k(UpdateType.ADD, streams)
        else:
            # No streams so store a "blank" config
            message = serialise_rf5k(UpdateType.REMOVEALL, streams)
        self._producer.produce(self._topic, bytes(message), int(time.time() * 1000))

    def retrieve_configuration(self):
        # Retrieve last valid configuration buffer from partition 0
        topic = TopicPartition(self._topic, 0)
        low_offset, high_offset = self._consumer.get_watermark_offsets(topic)
        # Set offset to high_offset - 1 to start retrieving from last message
        topic.offset = high_offset - 1
        self._consumer.assign([topic])

        stored_config_buffer = None

        while high_offset >= low_offset:
            msg = self._consumer.consume(timeout=2)
            if msg:
                payload = msg[-1].value()
                if self._is_a_valid_configuration_buffer(payload):
                    stored_config_buffer = payload
                    break
            high_offset -= 1
            self._consumer.seek(TopicPartition(self._topic, 0, high_offset))

        if stored_config_buffer:
            return stored_config_buffer
        else:
            raise RuntimeError("Could not retrieve stored configuration")

    def stop(self):
        self._producer.close()
        self._consumer.close()

    @staticmethod
    def _is_a_valid_configuration_buffer(payload):
        validated = True
        try:
            _ = deserialise_rf5k(payload)
        except Exception:
            validated = False
        return validated


NullConfigurationStore = mock.create_autospec(ConfigurationStore)
