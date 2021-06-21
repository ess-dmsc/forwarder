from random import randint

import numpy as np

from forwarder.kafka.kafka_producer import KafkaProducer
from forwarder.repeat_timer import RepeatTimer, milliseconds_to_seconds
from forwarder.update_handlers.schema_serialisers import schema_serialisers
from forwarder.kafka.kafka_helpers import _nanoseconds_to_milliseconds

from p4p.nt import NTScalar


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
            self._message_publisher = schema_serialisers[schema](self._pv_name)
        except KeyError:
            raise ValueError(
                f"{schema} is not a recognised supported schema, use one of {list(schema_serialisers.keys())}"
            )

        self._repeating_timer = RepeatTimer(
            milliseconds_to_seconds(fake_pv_period_ms), self._timer_callback
        )
        self._repeating_timer.start()

    def _timer_callback(self):
        if self._schema == "tdct":
            # tdct needs a 1D array as data to send
            data = np.array([randint(0, 100)]).astype(np.int32)
            update = NTScalar("ai").wrap(data)
        else:
            # Otherwise 0D (scalar) is fine
            update = NTScalar("i").wrap(randint(0, 100))
        self._publish_message(*self._message_publisher.serialise(update))

    def _publish_message(self, message: bytes, timestamp_ns: int):
        self._producer.produce(
            self._output_topic, message, _nanoseconds_to_milliseconds(timestamp_ns)
        )

    def stop(self):
        """
        Stop periodic updates
        """
        self._repeating_timer.cancel()
