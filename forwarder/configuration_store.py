import json
import time
from typing import Dict
from confluent_kafka import TopicPartition

from forwarder.update_handlers.ca_update_handler import CAUpdateHandler


class ConfigurationStore:
    def __init__(self, producer, consumer, topic):
        self._producer = producer
        self._consumer = consumer
        self._topic = topic

    def save_configuration(self, update_handlers: Dict):
        streams = []
        for name, update_handler in update_handlers.items():
            channel = {
                "channel": name,
                "converter": {
                    "topic": update_handler.output_topic,
                    "schema": update_handler.schema,
                },
            }

            if isinstance(update_handler, CAUpdateHandler):
                channel["channel_provider_type"] = "ca"

            streams.append(channel)
        message = json.dumps(streams).encode("utf-8")
        self._producer.produce(self._topic, message, int(time.time() * 1000))

    def retrieve_configuration(self):
        # Retrieve last message
        topic = TopicPartition(self._topic, 0)
        _, high_offset = self._consumer.get_watermark_offsets(topic)
        topic.offset = high_offset - 1
        self._consumer.assign([topic])

        msg = self._consumer.consume(timeout=2)

        if msg:
            config_msg = {"cmd": "add", "streams": json.loads(msg[~0].value())}
            return json.dumps(config_msg).encode("utf-8")
        else:
            raise RuntimeError("Could not retrieve stored configuration")

    def stop(self):
        self._producer.close()
        self._consumer.close()
