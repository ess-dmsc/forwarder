from applicationlogger import get_logger
from kafka.kafkahelpers import publish_f142_message
from kafka.aioproducer import AIOProducer
from caproto import ReadNotifyResponse
from caproto.threading.client import PV


class UpdateHandler:
    def __init__(self, producer: AIOProducer, pv: PV):
        self.logger = get_logger()
        self.producer = producer
        self.pv = pv
        self.cached_update = None

    def monitor_callback(self, response: ReadNotifyResponse):
        self.logger.debug(f"Received PV update {response.header}")
        publish_f142_message(
            self.producer, "forwarder-output", response.data,
        )
        self.cached_update = response.data

    def publish_cached_update(self):
        if self.cached_update is not None:
            publish_f142_message(
                self.producer, "forwarder-output", self.cached_update.data,
            )
