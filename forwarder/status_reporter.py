from forwarder.repeat_timer import RepeatTimer, milliseconds_to_seconds
from forwarder.kafka.kafka_producer import KafkaProducer
from typing import Dict
from streaming_data_types.status_x5f2 import serialise_x5f2
import json
import time
from socket import gethostname
from os import getpid
from logging import Logger
from forwarder.update_handlers.create_update_handler import UpdateHandler
from forwarder.parse_config_update import Channel


class StatusReporter:
    def __init__(
        self,
        update_handlers: Dict[Channel, UpdateHandler],
        producer: KafkaProducer,
        topic: str,
        service_id: str,
        version: str,
        logger: Logger,
        interval_ms: int = 4000,
    ):
        self._repeating_timer = RepeatTimer(
            milliseconds_to_seconds(interval_ms), self.report_status
        )
        self._producer = producer
        self._topic = topic
        self._update_handlers = update_handlers
        self._service_id = service_id
        self._interval_ms = interval_ms
        self._version = version
        self._logger = logger

    def start(self):
        self._repeating_timer.start()

    def report_status(self):
        status_json = json.dumps(
            {
                "streams": [
                    {
                        "channel_name": channel.name,
                        "protocol": channel.protocol.name,
                        "output_topic": channel.output_topic,
                        "schema": channel.schema,
                    }
                    for channel in self._update_handlers.keys()
                ]
            }
        )
        status_message = serialise_x5f2(
            "Forwarder",
            self._version,
            self._service_id,
            gethostname(),
            getpid(),
            self._interval_ms,
            status_json,
        )
        self._producer.produce(
            self._topic, bytes(status_message), int(time.time() * 1000)
        )
        self._logger.debug(status_json)

    def stop(self):
        self._producer.close()
        if self._repeating_timer is not None:
            self._repeating_timer.cancel()
