from queue import Queue
from typing import Optional, Union

from caproto.threading.client import Context as CAContext
from p4p.client.thread import Context as PVAContext

from forwarder.parse_config_update import EpicsProtocol
from forwarder.parse_config_update import Channel as ConfigChannel
from forwarder.kafka.kafka_producer import KafkaProducer
from forwarder.update_handlers.ca_update_handler import CAUpdateHandler
from forwarder.update_handlers.pva_update_handler import PVAUpdateHandler
from forwarder.update_handlers.fake_update_handler import FakeUpdateHandler


UpdateHandler = Union[CAUpdateHandler, PVAUpdateHandler, FakeUpdateHandler]


def create_update_handler(
    producer: KafkaProducer,
    ca_context: CAContext,
    pva_context: PVAContext,
    channel: ConfigChannel,
    fake_pv_period_ms: int,
    periodic_update_ms: Optional[int] = None,
    update_msg_queue: Optional[Queue] = None,
) -> UpdateHandler:
    if not channel.name:
        raise RuntimeError("PV name not specified when adding handler for channel")
    if not channel.output_topic:
        raise RuntimeError(
            f"Output topic not specified when adding handler for channel {channel.name}"
        )
    if not channel.schema:
        raise RuntimeError(
            f"Schema not specified when adding handler for channel {channel.name}"
        )
    if channel.protocol == EpicsProtocol.PVA:
        return PVAUpdateHandler(
            producer,
            pva_context,
            channel.name,
            channel.output_topic,
            channel.schema,
            periodic_update_ms,
            update_msg_queue,
        )
    elif channel.protocol == EpicsProtocol.CA:
        return CAUpdateHandler(
            producer,
            ca_context,
            channel.name,
            channel.output_topic,
            channel.schema,
            periodic_update_ms,
            update_msg_queue,
        )
    elif channel.protocol == EpicsProtocol.FAKE:
        return FakeUpdateHandler(
            producer,
            channel.name,
            channel.output_topic,
            channel.schema,
            fake_pv_period_ms,
            update_msg_queue,
        )
    raise RuntimeError("Unexpected EpicsProtocol in create_update_handler")
