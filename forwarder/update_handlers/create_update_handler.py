from typing import Optional, Union

from caproto.threading.client import Context as CAContext
from p4p.client.thread import Context as PVAContext

from forwarder.kafka.kafka_producer import KafkaProducer
from forwarder.parse_config_update import Channel as ConfigChannel
from forwarder.parse_config_update import EpicsProtocol
from forwarder.update_handlers.ca_update_handler import CAUpdateHandler
from forwarder.update_handlers.fake_update_handler import FakeUpdateHandler
from forwarder.update_handlers.pva_update_handler import PVAUpdateHandler
from forwarder.update_handlers.serialiser_tracker import create_serialiser_list

UpdateHandler = Union[CAUpdateHandler, PVAUpdateHandler, FakeUpdateHandler]


def create_update_handler(
    producer: KafkaProducer,
    ca_context: CAContext,
    pva_context: PVAContext,
    channel: ConfigChannel,
    fake_pv_period_ms: int,
    periodic_update_ms: Optional[int] = None,
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
    if channel.protocol == EpicsProtocol.NONE:
        raise RuntimeError(
            f"Protocol not specified when adding handler for channel {channel.name}"
        )
    serialiser_list = create_serialiser_list(
        producer,
        channel.name,
        channel.output_topic,
        channel.schema,
        channel.protocol,
        periodic_update_ms,
    )
    if channel.protocol == EpicsProtocol.PVA:
        return PVAUpdateHandler(pva_context, channel.name, serialiser_list)
    elif channel.protocol == EpicsProtocol.CA:
        return CAUpdateHandler(ca_context, channel.name, serialiser_list)
    elif channel.protocol == EpicsProtocol.FAKE:
        return FakeUpdateHandler(serialiser_list, channel.schema, fake_pv_period_ms)
    raise RuntimeError("Unexpected EpicsProtocol in create_update_handler")
