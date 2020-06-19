from forwarder.parse_config_update import EpicsProtocol
from forwarder.parse_config_update import Channel as ConfigChannel
from forwarder.kafka.kafka_producer import KafkaProducer
from typing import Optional
from caproto.threading.client import Context as CAContext
from p4p.client.thread import Context as PVAContext
from forwarder.update_handlers.base_update_handler import BaseUpdateHandler
from forwarder.update_handlers.ca_update_handler import CAUpdateHandler
from forwarder.update_handlers.pva_update_handler import PVAUpdateHandler
from forwarder.update_handlers.fake_update_handler import FakeUpdateHandler


def create_update_handler(
    producer: KafkaProducer,
    ca_context: CAContext,
    pva_context: PVAContext,
    channel: ConfigChannel,
    fake_pv_period_ms: int,
    periodic_update_ms: Optional[int] = None,
):
    if channel.protocol == EpicsProtocol.PVA:
        return BaseUpdateHandler(
            producer,
            pva_context,
            channel.name,
            channel.output_topic,
            channel.schema,
            PVAUpdateHandler(),
            periodic_update_ms,
        )
    elif channel.protocol == EpicsProtocol.CA:
        return BaseUpdateHandler(
            producer,
            ca_context,
            channel.name,
            channel.output_topic,
            channel.schema,
            CAUpdateHandler(),
            periodic_update_ms,
        )
    elif channel.protocol == EpicsProtocol.FAKE:
        return FakeUpdateHandler(
            producer,
            channel.name,
            channel.output_topic,
            channel.schema,
            fake_pv_period_ms,
        )
