from applicationlogger import get_logger
from kafka.kafkahelpers import publish_f142_message
from kafka.aioproducer import AIOProducer
from caproto import ReadNotifyResponse, ChannelType
from caproto.threading.client import PV
import numpy as np
import asyncio
from threading import Lock

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
        periodic_update_ms: int = 500,
    ):
        self._logger = get_logger()
        self._producer = producer
        self._pv = pv
        sub = self._pv.subscribe()
        sub.add_callback(self._monitor_callback)
        self._cached_update = None
        self._output_type = None
        self._cancelled = False

        try:
            self._message_publisher = schema_publishers[schema]
        except KeyError:
            raise ValueError(
                f"{schema} is not a recognised supported schema, use one of {list(schema_publishers.keys())}"
            )

        if periodic_update_ms != 0:
            self._cache_lock = Lock()
            self._periodic_update_s = float(periodic_update_ms) / 1000
            self._task = asyncio.ensure_future(self._do_periodic_update())

    def _monitor_callback(self, response: ReadNotifyResponse):
        self._logger.debug(f"Received PV update {response.header}")
        if self._output_type is None:
            try:
                self._output_type = _numpy_type_from_channel_type[response.data_type]
            except KeyError:
                self._logger.warning(
                    f"Don't know what numpy dtype to use for channel type {response.data_type}"
                )
        self._message_publisher(
            self._producer,
            "forwarder-output",
            np.squeeze(response.data).astype(self._output_type),
        )
        with self._cache_lock:
            self._cached_update = np.squeeze(response.data).astype(self._output_type)

    async def _publish_cached_update(self):
        self._logger.debug("Doing periodic update")
        with self._cache_lock:
            if self._cached_update is not None:
                publish_f142_message(
                    self._producer, "forwarder-output", self._cached_update,
                )

    async def _do_periodic_update(self):
        try:
            self._logger.debug("Starting periodic update")
            while not self._cancelled:
                await asyncio.sleep(self._periodic_update_s)
                await self._publish_cached_update()
        except Exception as ex:
            print(ex)

    def cancel(self):
        self._cancelled = True
        self._task.cancel()
