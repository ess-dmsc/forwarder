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
        # Retrieve last message
        topic = TopicPartition(self._topic, 0)
        _, high_offset = self._consumer.get_watermark_offsets(topic)
        topic.offset = high_offset - 1
        self._consumer.assign([topic])

        msg = self._consumer.consume(timeout=2)

        if msg:
            return msg[~0].value()
        else:
            raise RuntimeError("Could not retrieve stored configuration")

    def stop(self):
        self._producer.close()
        self._consumer.close()


NullConfigurationStore = mock.create_autospec(ConfigurationStore)
