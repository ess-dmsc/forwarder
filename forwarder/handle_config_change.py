import fnmatch
from logging import Logger
from typing import Dict, Optional

from caproto.threading.client import Context as CaContext
from p4p.client.thread import Context as PvaContext

from forwarder.configuration_store import ConfigurationStore, NullConfigurationStore
from forwarder.kafka.kafka_producer import KafkaProducer
from forwarder.parse_config_update import Channel, CommandType, ConfigUpdate
from forwarder.status_reporter import StatusReporter
from forwarder.update_handlers.create_update_handler import (
    UpdateHandler,
    create_update_handler,
)


def _subscribe_to_pv(
    new_channel: Channel,
    update_handlers: Dict[Channel, UpdateHandler],
    producer: KafkaProducer,
    ca_ctx: CaContext,
    pva_ctx: PvaContext,
    logger: Logger,
    fake_pv_period: int,
    pv_update_period: Optional[int],
):
    if new_channel in update_handlers.keys():
        logger.warning(
            "Forwarder asked to subscribe to PV it is already has an identical "
            "configuration for"
        )
        return

    try:
        update_handlers[new_channel] = create_update_handler(
            producer,
            ca_ctx,
            pva_ctx,
            new_channel,
            fake_pv_period,
            periodic_update_ms=pv_update_period,
        )
    except RuntimeError as error:
        logger.error(str(error))
    logger.info(
        f"Subscribed to PV name='{new_channel.name}', schema='{new_channel.schema}', topic='{new_channel.output_topic}'"
    )


def _unsubscribe_from_pv(
    remove_channel: Channel,
    update_handlers: Dict[Channel, UpdateHandler],
    logger: Logger,
):
    def _match_channel_field(
        field_in_remove_request: Optional[str], field_in_existing_channel: Optional[str]
    ) -> bool:
        return (
            True
            if not field_in_remove_request
            or field_in_existing_channel == field_in_remove_request
            else False
        )

    def _wildcard_match_channel_field(
        field_in_remove_request: Optional[str], field_in_existing_channel: Optional[str]
    ) -> bool:
        return (
            True
            if not field_in_remove_request
            or fnmatch.fnmatch(field_in_existing_channel, field_in_remove_request)  # type: ignore
            else False
        )

    channels_to_remove = []
    for channel in update_handlers.keys():
        matching_fields = (
            _wildcard_match_channel_field(remove_channel.name, channel.name),
            _match_channel_field(remove_channel.schema, channel.schema),
            _wildcard_match_channel_field(
                remove_channel.output_topic, channel.output_topic
            ),
        )
        if all(matching_fields):
            channels_to_remove.append(channel)

    for channel in channels_to_remove:
        update_handlers[channel].stop()
        del update_handlers[channel]

    logger.info(
        f"Unsubscribed from PVs matching name='{remove_channel.name}', schema='{remove_channel.schema}', topic='{remove_channel.output_topic}'"
    )


def _unsubscribe_from_all(
    update_handlers: Dict[Channel, UpdateHandler], logger: Logger
):
    for update_handler in update_handlers.values():
        update_handler.stop()
    update_handlers.clear()
    logger.info("Unsubscribed from all PVs")


def handle_configuration_change(
    configuration_change: ConfigUpdate,
    fake_pv_period: int,
    pv_update_period: Optional[int],
    update_handlers: Dict[Channel, UpdateHandler],
    producer: KafkaProducer,
    ca_ctx: CaContext,
    pva_ctx: PvaContext,
    logger: Logger,
    status_reporter: StatusReporter,
    configuration_store: ConfigurationStore = NullConfigurationStore,
):
    """
    Add or remove update handlers according to the requested change in configuration
    """
    if configuration_change.command_type == CommandType.REMOVE_ALL:
        _unsubscribe_from_all(update_handlers, logger)
    elif configuration_change.command_type == CommandType.INVALID:
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
                    _unsubscribe_from_pv(channel, update_handlers, logger)
    status_reporter.report_status()
    configuration_store.save_configuration(update_handlers)
