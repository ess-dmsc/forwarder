from forwarder.kafka.kafka_helpers import create_producer
from forwarder.repeat_timer import RepeatTimer, milliseconds_to_seconds
from typing import Dict
import json
import time


class StatusReporter:
    def __init__(
        self, update_handlers: Dict, broker: str, topic: str, interval_ms: int = 4000
    ):
        self._repeating_timer = RepeatTimer(
            milliseconds_to_seconds(interval_ms), self.report_status
        )
        self._producer = create_producer(broker)
        self._topic = topic
        self._update_handlers = update_handlers

    def start(self):
        self._repeating_timer.start()

    def report_status(self):
        status_message = json.dumps(
            {
                "streams": [
                    {"channel_name": channel_name}
                    for channel_name in self._update_handlers.keys()
                ]
            }
        ).encode("utf-8")
        self._producer.produce(self._topic, status_message, int(time.time() * 1000))

    def stop(self):
        self._producer.close()
        if self._repeating_timer is not None:
            self._repeating_timer.cancel()
