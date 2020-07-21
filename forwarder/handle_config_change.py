from forwarder.update_handlers.create_update_handler import create_update_handler
from forwarder.parse_config_update import (
    CommandType,
    Channel,
    ConfigUpdate,
)
from typing import Optional, Dict
from logging import Logger
from forwarder.status_reporter import StatusReporter
from caproto.threading.client import Context as CaContext
from p4p.client.thread import Context as PvaContext
from forwarder.kafka.kafka_producer import KafkaProducer


def _subscribe_to_pv(
    new_channel: Channel,
    update_handlers: Dict,
    producer: KafkaProducer,
    ca_ctx: CaContext,
    pva_ctx: PvaContext,
    logger: Logger,
    fake_pv_period: int,
    pv_update_period: Optional[int],
):
    if new_channel.name in update_handlers.keys():
        logger.warning("Forwarder asked to subscribe to PV it is already subscribed to")
        return

    try:
        update_handlers[new_channel.name] = create_update_handler(
            producer,
            ca_ctx,
            pva_ctx,
            new_channel,
            fake_pv_period,
            periodic_update_ms=pv_update_period,
        )
    except RuntimeError as error:
        logger.error(str(error))
    logger.info(f"Subscribed to PV {new_channel.name}")


def _unsubscribe_from_pv(name: str, update_handlers: Dict, logger: Logger):
    try:
        update_handlers[name].stop()
        del update_handlers[name]
    except KeyError:
        logger.warning(
            "Forwarder asked to unsubscribe from a PV it is not subscribed to"
        )
    logger.info(f"Unsubscribed from PV {name}")


def _unsubscribe_from_all(update_handlers: Dict, logger: Logger):
    for _, update_handler in update_handlers.items():
        update_handler.stop()
    update_handlers.clear()
    logger.info("Unsubscribed from all PVs")


def handle_configuration_change(
    configuration_change: ConfigUpdate,
    fake_pv_period: int,
    pv_update_period: Optional[int],
    update_handlers: Dict,
    producer: KafkaProducer,
    ca_ctx: CaContext,
    pva_ctx: PvaContext,
    logger: Logger,
    status_reporter: StatusReporter,
):
    """
    Add or remove update handlers according to the requested change in configuration
    """
    if configuration_change.command_type == CommandType.REMOVE_ALL:
        _unsubscribe_from_all(update_handlers, logger)
    elif configuration_change.command_type == CommandType.MALFORMED:
        return
    else:
        if configuration_change.channels is not None:
            for channel in configuration_change.channels:
                if configuration_change.command_type == CommandType.ADD:
                    _subscribe_to_pv(
                        channel,
                        update_handlers,
                        producer,
                        ca_ctx,
                        pva_ctx,
                        logger,
                        fake_pv_period,
                        pv_update_period,
                    )
                elif configuration_change.command_type == CommandType.REMOVE:
                    _unsubscribe_from_pv(channel.name, update_handlers, logger)
    status_reporter.report_status()
