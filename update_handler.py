from application_logger import get_logger
from kafka.kafka_helpers import publish_f142_message
from kafka.aio_producer import AIOProducer
from caproto import ReadNotifyResponse, ChannelType
from caproto.threading.client import PV
import numpy as np
from threading import Lock, Event, Timer
from epics_to_serialisable_types import numpy_type_from_channel_type

schema_publishers = {"f142": publish_f142_message}
output_topic = "forwarder-output"


class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


def _milliseconds_to_seconds(time_ms: int) -> float:
    return float(time_ms) / 1000


class UpdateHandler:
    def __init__(
        self,
        producer: AIOProducer,
        pv: PV,
        schema: str = "f142",
        periodic_update_ms: int = 0,
    ):
        self._logger = get_logger()
        self._producer = producer
        self._pv = pv
        # Subscribe with "data_type='control'" otherwise we don't get the metadata with alarm fields
        sub = self._pv.subscribe(data_type="control")
        sub.add_callback(self._monitor_callback)
        self._cached_update = None
        self._output_type = None
        self._stop_timer_flag = Event()
        self._repeating_timer = None
        self._cache_lock = Lock()

        try:
            self._message_publisher = schema_publishers[schema]
        except KeyError:
            raise ValueError(
                f"{schema} is not a recognised supported schema, use one of {list(schema_publishers.keys())}"
            )

        if periodic_update_ms != 0:
            self._repeating_timer = RepeatTimer(
                _milliseconds_to_seconds(periodic_update_ms), self.publish_cached_update
            )
            self._repeating_timer.start()

    def _monitor_callback(self, response: ReadNotifyResponse):
        self._logger.debug(
            f"Received PV update, STATUS: {response.metadata.status}, SEVERITY: {response.metadata.severity}, METADATA: {response.metadata}"
        )
        if self._output_type is None:
            try:
                self._output_type = numpy_type_from_channel_type[response.data_type]
            except KeyError:
                self._logger.warning(
                    f"Don't know what numpy dtype to use for channel type {ChannelType(response.data_type)}"
                )

        # TODO get timestamp from EPICS for kafka_timestamp
        #  map caproto EPICS alarm enums to ones in f142
        self._logger.info("before lock")
        with self._cache_lock:
            self._logger.info("after lock")
            if (
                self._cached_update is None
                or response.metadata.status != self._cached_update.metadata.status
            ):
                self._message_publisher(
                    self._producer,
                    output_topic,
                    np.squeeze(response.data).astype(self._output_type),
                    source_name=self._pv.name,
                    kafka_timestamp=42,
                    alarm_status=response.metadata.status,
                    alarm_severity=response.metadata.severity,
                )
            else:
                self._message_publisher(
                    self._producer,
                    output_topic,
                    np.squeeze(response.data).astype(self._output_type),
                    source_name=self._pv.name,
                    kafka_timestamp=42,
                )
            self._cached_update = response

    def publish_cached_update(self):
        with self._cache_lock:
            if self._cached_update is not None:
                self._message_publisher(
                    self._producer,
                    output_topic,
                    np.squeeze(self._cached_update.data).astype(self._output_type),
                    source_name=self._pv.name,
                    kafka_timestamp=42,
                )

    def stop(self):
        """
        Stop periodic updates and unsubscribe from PV
        """
        if self._repeating_timer is not None:
            self._repeating_timer.cancel()
        self._pv.unsubscribe_all()
