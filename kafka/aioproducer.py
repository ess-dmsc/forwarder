import asyncio
import confluent_kafka
from threading import Thread


class AIOProducer:
    """
    From https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/asyncio.py
    """
    def __init__(self, configs, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._producer = confluent_kafka.Producer(configs)
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    def _poll_loop(self):
        while not self._cancelled:
            self._producer.poll(0.1)

    def close(self):
        self._cancelled = True
        self._poll_thread.join()

    def produce(self, topic, value):

        def ack(err, msg):
            if err:
                print(f'%% Message failed delivery: {err}')
            else:
                print('%% Message delivered to %s [%d] @ %d\n' %
                      (msg.topic(), msg.partition(), msg.offset()))

        self._producer.produce(topic, value, on_delivery=ack)
        self._producer.poll(0)
