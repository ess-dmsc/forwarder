from parse_config_update import EpicsProtocol
from parse_config_update import Channel as ConfigChannel
from kafka.aio_producer import AIOProducer
from typing import Optional
from caproto.threading.client import Context as CAContext
from p4p.client.thread import Context as PVAContext
from kafka.kafka_helpers import publish_f142_message
from update_handlers.ca_update_handler import CAUpdateHandler
from update_handlers.pva_update_handler import PVAUpdateHandler


schema_publishers = {"f142": publish_f142_message}


def create_update_handler(
    producer: AIOProducer,
    ca_context: CAContext,
    pva_context: PVAContext,
    channel: ConfigChannel,
    schema: str = "f142",
    periodic_update_ms: Optional[int] = None,
):
    if channel.protocol == EpicsProtocol.PVA:
        return PVAUpdateHandler(pva_context, channel.name)
    elif channel.protocol == EpicsProtocol.CA:
        return CAUpdateHandler(
            producer,
            ca_context,
            channel.name,
            channel.output_topic,
            schema,
            periodic_update_ms,
        )
