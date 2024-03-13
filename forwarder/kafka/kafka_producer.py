from threading import Thread
from typing import Optional

from confluent_kafka import KafkaError, Message, Producer

from forwarder.application_logger import get_logger
from forwarder.metrics import Counter, Summary


class KafkaProducer:
    def __init__(
        self,
        producer: Producer,
        update_msg_counter: Optional[Counter] = None,
        update_buffer_err_counter: Optional[Counter] = None,
        update_delivery_err_counter: Optional[Counter] = None,
        latency_metric: Optional[Summary] = None,
    ):
        self._producer = producer
        self._update_msg_counter = update_msg_counter
        self._update_buffer_err_counter = update_buffer_err_counter
        self._update_delivery_err_counter = update_delivery_err_counter
        self._latency_metric = latency_metric
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()
        self.logger = get_logger()

    def _poll_loop(self):
        try:
            while not self._cancelled:
                self._producer.poll(0.5)
        except BaseException as e:
            self.logger.exception(e)

    def close(self):
        self._cancelled = True
        self._poll_thread.join()
        max_wait_to_publish_producer_queue = 2  # seconds
        self._producer.flush(max_wait_to_publish_producer_queue)

    def produce(
        self, topic: str, payload: bytes, timestamp_ms: int, key: Optional[str] = None
    ):
        def ack(err: KafkaError, msg: Message):
            if err:
                self.logger.error(f"Message failed delivery: {err}")
                if self._update_delivery_err_counter:
                    self._update_delivery_err_counter.inc()
            else:
                # increment only for PVs related updates
                # key is None when we send commands.
                if self._update_msg_counter and key is not None:
                    self._update_msg_counter.inc()
                if self._latency_metric and key is not None:
                    self._latency_metric.observe(msg.latency())

        try:
            self._producer.produce(
                topic, payload, key=key, on_delivery=ack, timestamp=timestamp_ms
            )
        except BufferError:
            # Producer message buffer is full.
            # Data loss occurred as messages are produced faster than are sent to the kafka broker.
            if self._update_buffer_err_counter:
                self._update_buffer_err_counter.inc()
        self._producer.poll(0)
