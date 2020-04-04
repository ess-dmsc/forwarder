from applicationlogger import get_logger
from kafka.kafkahelpers import publish_f142_message
from kafka.aioproducer import AIOProducer
from caproto import ReadNotifyResponse, ChannelType
from caproto.threading.client import PV
import numpy as np

# caproto can give us values of different dtypes even from the same EPICS channel,
# for example it will use the smallest integer type it can for the particular value,
# for example ">i2" (big-endian, 2 byte int).
# Unfortunately the serialisation method doesn't know what to do with such a specific dtype
# so we will cast to a consistent type based on the EPICS channel type.
_numpy_type_from_channel_type = {
    ChannelType.INT: np.int32,
    ChannelType.LONG: np.int64,
    ChannelType.FLOAT: np.float,
    ChannelType.DOUBLE: np.float64,
    ChannelType.STRING: np.unicode_,
}

schema_publishers = {"f142": publish_f142_message}


class UpdateHandler:
    def __init__(
        self,
        producer: AIOProducer,
        pv: PV,
        schema: str = "f142",
        periodic_update: int = 0,
    ):
        self.logger = get_logger()
        self.producer = producer
        self.pv = pv
        sub = self.pv.subscribe()
        sub.add_callback(self.monitor_callback)
        self.cached_update = None
        self.output_type = None

        try:
            self.message_publisher = schema_publishers[schema]
        except KeyError:
            raise ValueError(
                f"{schema} is not a recognised supported schema, use one of {list(schema_publishers.keys())}"
            )

        if periodic_update != 0:
            pass

    def monitor_callback(self, response: ReadNotifyResponse):
        self.logger.debug(f"Received PV update {response.header}")
        if self.output_type is None:
            try:
                self.output_type = _numpy_type_from_channel_type[response.data_type]
            except KeyError:
                self.logger.warning(
                    f"Don't know what numpy dtype to use for channel type {response.data_type}"
                )
        self.message_publisher(
            self.producer,
            "forwarder-output",
            np.squeeze(response.data).astype(self.output_type),
        )
        self.cached_update = response.data

    def publish_cached_update(self):
        if self.cached_update is not None:
            publish_f142_message(
                self.producer, "forwarder-output", self.cached_update.data,
            )
