import confluent_kafka
from threading import Thread
from application_logger import setup_logger


class AIOProducer:
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

    def produce(self, topic: str, payload: bytes):
        def ack(err, msg):
            if err:
                self.logger.error(f"Message failed delivery: {err}")
            else:
                self.logger.debug(
                    f"Message delivered to {msg.topic()} {msg.partition()} @ {msg.offset()}"
                )

        self._producer.produce(topic, payload, on_delivery=ack)
        self._producer.poll(0)
