from forwarder.kafka.kafka_producer import KafkaProducer
import numpy as np
from forwarder.repeat_timer import RepeatTimer, milliseconds_to_seconds
import time
from forwarder.update_handlers.schema_publishers import schema_publishers
from random import randint


class FakeUpdateHandler:
    """
    Periodically generate a random integer as a PV value instead of monitoring a real EPICS PV
    serialises updates in FlatBuffers and passes them onto an Kafka Producer.
    """

    def __init__(
        self,
        producer: KafkaProducer,
        pv_name: str,
        output_topic: str,
        schema: str,
        fake_pv_period_ms: int,
    ):
        self._producer = producer
        self._output_topic = output_topic
        self._pv_name = pv_name
        self._schema = schema

        try:
            self._message_publisher = schema_publishers[schema]
        except KeyError:
            raise ValueError(
                f"{schema} is not a recognised supported schema, use one of {list(schema_publishers.keys())}"
            )

        self._repeating_timer = RepeatTimer(
            milliseconds_to_seconds(fake_pv_period_ms), self._timer_callback
        )
        self._repeating_timer.start()

    def _timer_callback(self):
        if self._schema == "tdct":
            # tdct needs a 1D array as data to send
            data = np.array([randint(0, 100)]).astype(np.int32)
        else:
            # Otherwise 0D (scalar) is fine
            data = np.array(randint(0, 100)).astype(np.int32)
        self._message_publisher(
            self._producer, self._output_topic, data, self._pv_name, time.time_ns(),
        )

    def stop(self):
        """
        Stop periodic updates
        """
        self._repeating_timer.cancel()
