from forwarder.handle_config_change import handle_configuration_change
from tests.kafka.fake_producer import FakeProducer
import logging
from forwarder.parse_config_update import (
    ConfigUpdate,
    CommandType,
    Channel,
    EpicsProtocol,
)
from forwarder.update_handlers.create_update_handler import UpdateHandler
from typing import Dict, List
import pytest


class StubStatusReporter:
    def report_status(self):
        pass


class StubUpdateHandler:
    def stop(self):
        pass


_logger = logging.getLogger("stub_for_use_in_tests")
_logger.addHandler(logging.NullHandler())


def _get_channel_names(update_handlers: Dict[Channel, UpdateHandler]) -> List[str]:
    return [channel.name for channel in update_handlers.keys()]


@pytest.fixture(scope="function")
def update_handlers():
    """
    Fixture for creating update handlers to ensure they get cleaned up whether the test passes or not
    """
    update_handlers: Dict[Channel, UpdateHandler] = {}
    yield update_handlers

    # Clean up
    for _, handler in update_handlers.items():
        handler.stop()


def test_no_change_to_empty_update_handlers_when_malformed_config_update_handled(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()
    config_update = ConfigUpdate(CommandType.MALFORMED, None)

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert not update_handlers


def test_no_change_to_update_handlers_when_malformed_config_update_handled(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()
    config_update = ConfigUpdate(CommandType.MALFORMED, None)

    existing_channel_name = "test_channel"
    update_handlers[Channel(existing_channel_name, EpicsProtocol.NONE, None, None)] = StubUpdateHandler()  # type: ignore
    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 1
    assert existing_channel_name in _get_channel_names(update_handlers)


def test_all_update_handlers_are_removed_when_removeall_config_update_is_handled(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()
    config_update = ConfigUpdate(CommandType.REMOVE_ALL, None)

    update_handlers[Channel("test_channel_1", EpicsProtocol.NONE, None, None)] = StubUpdateHandler()  # type: ignore
    update_handlers[Channel("test_channel_2", EpicsProtocol.NONE, None, None)] = StubUpdateHandler()  # type: ignore
    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert not update_handlers


def test_update_handlers_are_removed_when_remove_config_update_is_handled(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()
    channel_name_1 = "test_channel_1"
    channel_name_2 = "test_channel_2"
    config_update = ConfigUpdate(
        CommandType.REMOVE, (Channel(channel_name_1, EpicsProtocol.NONE, None, None),),
    )

    update_handlers[Channel(channel_name_1, EpicsProtocol.NONE, None, None)] = StubUpdateHandler()  # type: ignore
    update_handlers[Channel(channel_name_2, EpicsProtocol.NONE, None, None)] = StubUpdateHandler()  # type: ignore
    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 1
    assert channel_name_2 in _get_channel_names(
        update_handlers
    ), "Expected handler for channel_name_1 to have been removed, leaving only channel_name_2"


def test_update_handlers_are_added_when_add_config_update_is_handled(update_handlers):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()
    channel_name_1 = "test_channel_1"
    channel_name_2 = "test_channel_2"
    config_update = ConfigUpdate(
        CommandType.ADD,
        (
            Channel(channel_name_1, EpicsProtocol.FAKE, "output_topic", "f142"),
            Channel(channel_name_2, EpicsProtocol.FAKE, "output_topic", "f142"),
        ),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 2
    assert channel_name_1 in _get_channel_names(update_handlers)
    assert channel_name_2 in _get_channel_names(update_handlers)


def test_can_add_multiple_channels_with_same_name_if_protocol_topic_or_schema_are_different(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()
    channel_name = "test_channel"
    test_channel_1 = Channel(channel_name, EpicsProtocol.FAKE, "output_topic", "f142")
    test_channel_2 = Channel(channel_name, EpicsProtocol.FAKE, "output_topic_2", "f142")
    test_channel_3 = Channel(channel_name, EpicsProtocol.FAKE, "output_topic", "tdct")
    config_update = ConfigUpdate(
        CommandType.ADD, (test_channel_1, test_channel_2, test_channel_3,),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 3
    assert test_channel_1 in update_handlers.keys()
    assert test_channel_2 in update_handlers.keys()
    assert test_channel_3 in update_handlers.keys()


def test_identical_configurations_are_not_added(update_handlers,):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()
    channel_name = "test_channel"
    test_channel_1 = Channel(channel_name, EpicsProtocol.FAKE, "output_topic", "f142")
    test_channel_2 = Channel(channel_name, EpicsProtocol.FAKE, "output_topic", "f142")
    test_channel_3 = Channel(channel_name, EpicsProtocol.FAKE, "output_topic", "f142")
    config_update = ConfigUpdate(
        CommandType.ADD, (test_channel_1, test_channel_2, test_channel_3,),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert (
        len(update_handlers) == 1
    ), "Only expect one channel to be added as others requested were identical"
    assert test_channel_1 in update_handlers.keys()
