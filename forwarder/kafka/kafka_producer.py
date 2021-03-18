from threading import Thread
from typing import Optional

import confluent_kafka

from forwarder.application_logger import setup_logger
from forwarder.utils import Counter


class KafkaProducer:
    def __init__(
        self,
        producer: confluent_kafka.Producer,
        update_msg_counter: Optional[Counter] = None,
        update_buffer_err_counter: Optional[Counter] = None,
    ):
        self._producer = producer
        self._update_msg_counter = update_msg_counter
        self._update_buffer_err_counter = update_buffer_err_counter
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
            else:
                # increment only for PVs related updates
                # key is None when we send commands.
                if self._update_msg_counter and key is not None:
                    self._update_msg_counter.increment()

        try:
            self._producer.produce(
                topic, payload, key=key, on_delivery=ack, timestamp=timestamp_ms
            )
        except BufferError:
            # Producer message buffer is full.
            # Data loss occurred as messages are produced faster than are sent to the kafka broker.
            if self._update_buffer_err_counter:
                self._update_buffer_err_counter.increment()
        self._producer.poll(0)
