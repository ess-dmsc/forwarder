import confluent_kafka
from threading import Thread
from forwarder.application_logger import setup_logger
from typing import Optional


class KafkaProducer:
    def __init__(self, configs: dict):
        self._producer = confluent_kafka.Producer(configs)
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()
        self.logger = setup_logger()

    def _poll_loop(self):
        while not self._cancelled:
            self._producer.poll(0.5)

    def close(self):
        self._cancelled = True
        self._poll_thread.join()
        max_wait_to_publish_producer_queue = 2  # seconds
        self._producer.flush(max_wait_to_publish_producer_queue)

    def produce(
        self, topic: str, payload: bytes, timestamp_ms: int, key: Optional[str] = None
    ):
        def ack(err, _):
            if err:
                self.logger.error(f"Message failed delivery: {err}")

        self._producer.produce(
            topic, payload, key=key, on_delivery=ack, timestamp=timestamp_ms
        )
        self._producer.poll(0)
