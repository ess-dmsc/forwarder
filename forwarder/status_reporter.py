from forwarder.repeat_timer import RepeatTimer, milliseconds_to_seconds
from forwarder.kafka.kafka_producer import KafkaProducer
from typing import Dict
import json
import time


class StatusReporter:
    def __init__(
        self,
        update_handlers: Dict,
        producer: KafkaProducer,
        topic: str,
        service_id: str,
        interval_ms: int = 4000,
    ):
        self._repeating_timer = RepeatTimer(
            milliseconds_to_seconds(interval_ms), self.report_status
        )
        self._producer = producer
        self._topic = topic
        self._update_handlers = update_handlers
        self._service_id = service_id

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
