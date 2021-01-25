from forwarder.configuration_store import ConfigurationStore
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
from unittest import mock


class StubStatusReporter:
    def report_status(self):
        pass


class StubUpdateHandler:
    def stop(self):
        pass


_logger = logging.getLogger("stub_for_use_in_tests")
_logger.addHandler(logging.NullHandler())


def _get_channel_names(update_handlers: Dict[Channel, UpdateHandler]) -> List[str]:
    return [
        channel.name for channel in update_handlers.keys() if channel.name is not None
    ]


@pytest.fixture(scope="function")
def update_handlers():
    """
    Fixture for creating update handlers to ensure they get cleaned up, whether the test passes or not
    """
    update_handlers: Dict[Channel, UpdateHandler] = {}
    yield update_handlers

    # Clean up
    for _, handler in update_handlers.items():
        handler.stop()


def test_no_change_to_empty_update_handlers_when_invalid_config_update_handled(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()
    config_update = ConfigUpdate(CommandType.INVALID, None)

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert not update_handlers


def test_no_change_to_update_handlers_when_invalid_config_update_handled(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()
    config_update = ConfigUpdate(CommandType.INVALID, None)

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
        CommandType.REMOVE,
        (Channel(channel_name_1, EpicsProtocol.NONE, None, None),),
    )

    update_handlers[Channel(channel_name_1, EpicsProtocol.NONE, None, None)] = StubUpdateHandler()  # type: ignore
    update_handlers[Channel(channel_name_2, EpicsProtocol.NONE, None, None)] = StubUpdateHandler()  # type: ignore
    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 1
    assert channel_name_2 in _get_channel_names(
        update_handlers
    ), "Expected handler for channel_name_1 to have been removed, leaving only channel_name_2"


def test_update_handlers_can_be_removed_by_topic(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()

    topic_name_1 = "topic_1"
    topic_name_2 = "topic_2"
    update_handlers[Channel("", EpicsProtocol.NONE, topic_name_1, None)] = StubUpdateHandler()  # type: ignore
    update_handlers[Channel("", EpicsProtocol.NONE, topic_name_2, None)] = StubUpdateHandler()  # type: ignore
    update_handlers[Channel("", EpicsProtocol.NONE, topic_name_1, None)] = StubUpdateHandler()  # type: ignore

    config_update = ConfigUpdate(
        CommandType.REMOVE,
        (Channel(None, EpicsProtocol.NONE, topic_name_1, None),),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 1
    assert (
        list(update_handlers.keys())[0].output_topic == topic_name_2
    ), "Expected handlers for topic_1 to have been removed, leaving only one for topic_2"


def test_update_handlers_can_be_removed_by_schema(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()

    schema_1 = "f142"
    schema_2 = "tdct"
    update_handlers[Channel("", EpicsProtocol.NONE, "", schema_1)] = StubUpdateHandler()  # type: ignore
    update_handlers[Channel("", EpicsProtocol.NONE, "", schema_2)] = StubUpdateHandler()  # type: ignore
    update_handlers[Channel("", EpicsProtocol.NONE, "", schema_1)] = StubUpdateHandler()  # type: ignore

    config_update = ConfigUpdate(
        CommandType.REMOVE,
        (Channel(None, EpicsProtocol.NONE, None, schema_1),),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 1
    assert (
        list(update_handlers.keys())[0].schema == schema_2
    ), "Expected handlers for schema_1 to have been removed, leaving only one for schema_2"


def test_update_handlers_can_be_removed_by_schema_and_topic(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()

    schema_1 = "f142"
    schema_2 = "tdct"
    topic_name_1 = "topic_1"
    topic_name_2 = "topic_2"
    test_channel_3 = Channel("", EpicsProtocol.NONE, topic_name_2, schema_1)
    update_handlers[Channel("", EpicsProtocol.NONE, topic_name_1, schema_1)] = StubUpdateHandler()  # type: ignore
    update_handlers[Channel("", EpicsProtocol.NONE, topic_name_1, schema_2)] = StubUpdateHandler()  # type: ignore
    update_handlers[test_channel_3] = StubUpdateHandler()  # type: ignore

    # Only the handler with the channel matching provided topic AND schema should be removed
    config_update = ConfigUpdate(
        CommandType.REMOVE,
        (Channel(None, EpicsProtocol.NONE, topic_name_2, schema_1),),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 2
    assert test_channel_3 not in update_handlers.keys()


def test_update_handlers_can_be_removed_by_name_and_topic(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()

    channel_name_1 = "channel_1"
    channel_name_2 = "channel_2"
    topic_name_1 = "topic_1"
    topic_name_2 = "topic_2"
    test_channel_3 = Channel(channel_name_1, EpicsProtocol.NONE, topic_name_2, None)
    update_handlers[Channel(channel_name_1, EpicsProtocol.NONE, topic_name_1, None)] = StubUpdateHandler()  # type: ignore
    update_handlers[Channel(channel_name_2, EpicsProtocol.NONE, topic_name_1, None)] = StubUpdateHandler()  # type: ignore
    update_handlers[test_channel_3] = StubUpdateHandler()  # type: ignore

    # Only the handler with the channel matching provided name AND topic should be removed
    config_update = ConfigUpdate(
        CommandType.REMOVE,
        (test_channel_3,),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 2
    assert test_channel_3 not in update_handlers.keys()


def test_update_handlers_can_be_removed_by_name_and_schema(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()

    schema_1 = "f142"
    schema_2 = "tdct"
    channel_name_1 = "channel_1"
    channel_name_2 = "channel_2"
    test_channel_3 = Channel(channel_name_2, EpicsProtocol.NONE, None, schema_1)
    update_handlers[Channel(channel_name_1, EpicsProtocol.NONE, None, schema_1)] = StubUpdateHandler()  # type: ignore
    update_handlers[Channel(channel_name_2, EpicsProtocol.NONE, None, schema_2)] = StubUpdateHandler()  # type: ignore
    update_handlers[test_channel_3] = StubUpdateHandler()  # type: ignore

    # Only the handler with the channel matching provided name AND schema should be removed
    config_update = ConfigUpdate(
        CommandType.REMOVE,
        (test_channel_3,),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 2
    assert test_channel_3 not in update_handlers.keys()


def test_single_character_wildcard_can_be_used_to_remove_channels_by_topic(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()

    test_channel_3 = Channel("channel_3", EpicsProtocol.NONE, "topic_III", None)
    update_handlers[Channel("channel_1", EpicsProtocol.NONE, "topic_1", None)] = StubUpdateHandler()  # type: ignore
    update_handlers[Channel("channel_2", EpicsProtocol.NONE, "topic_2", None)] = StubUpdateHandler()  # type: ignore
    update_handlers[test_channel_3] = StubUpdateHandler()  # type: ignore

    config_update = ConfigUpdate(
        CommandType.REMOVE,
        (Channel(None, EpicsProtocol.NONE, "topic_?", None),),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 1
    assert test_channel_3 in update_handlers.keys()


def test_multicharacter_wildcard_can_be_used_to_remove_channels_by_topic(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()

    test_channel_3 = Channel("channel_3", EpicsProtocol.NONE, "topic_3", None)
    update_handlers[Channel("channel_1", EpicsProtocol.NONE, "first_topic", None)] = StubUpdateHandler()  # type: ignore
    update_handlers[Channel("channel_2", EpicsProtocol.NONE, "second_topic", None)] = StubUpdateHandler()  # type: ignore
    update_handlers[test_channel_3] = StubUpdateHandler()  # type: ignore

    config_update = ConfigUpdate(
        CommandType.REMOVE,
        (Channel(None, EpicsProtocol.NONE, "*_topic", None),),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 1
    assert test_channel_3 in update_handlers.keys()


def test_single_character_wildcard_can_be_used_to_remove_channels_by_name(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()

    test_channel_3 = Channel("channel_III", EpicsProtocol.NONE, "topic_3", None)
    update_handlers[Channel("channel_1", EpicsProtocol.NONE, "topic_1", None)] = StubUpdateHandler()  # type: ignore
    update_handlers[Channel("channel_2", EpicsProtocol.NONE, "topic_2", None)] = StubUpdateHandler()  # type: ignore
    update_handlers[test_channel_3] = StubUpdateHandler()  # type: ignore

    config_update = ConfigUpdate(
        CommandType.REMOVE,
        (Channel("channel_?", EpicsProtocol.NONE, None, None),),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 1
    assert test_channel_3 in update_handlers.keys()


def test_multicharacter_wildcard_can_be_used_to_remove_channels_by_name(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()

    test_channel_3 = Channel("channel_3", EpicsProtocol.NONE, "topic_3", None)
    update_handlers[Channel("first_channel", EpicsProtocol.NONE, "topic2", None)] = StubUpdateHandler()  # type: ignore
    update_handlers[
        Channel("second_channel", EpicsProtocol.NONE, "topic 1", None)
    ] = StubUpdateHandler()  # type: ignore
    update_handlers[test_channel_3] = StubUpdateHandler()  # type: ignore

    config_update = ConfigUpdate(
        CommandType.REMOVE,
        (Channel("*_channel", EpicsProtocol.NONE, None, None),),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 1
    assert test_channel_3 in update_handlers.keys()


def test_wildcard_cannot_be_used_to_remove_channels_by_schema(
    update_handlers,
):
    # No wildcard matching on schemas because ? and * are allowed characters in schema identifiers

    status_reporter = StubStatusReporter()
    producer = FakeProducer()

    test_channel_3 = Channel("channel_3", EpicsProtocol.NONE, "topic_3", "f142")
    update_handlers[Channel("channel_1", EpicsProtocol.NONE, "topic_1", "f142")] = StubUpdateHandler()  # type: ignore
    update_handlers[Channel("channel_2", EpicsProtocol.NONE, "topic_2", "f142")] = StubUpdateHandler()  # type: ignore
    update_handlers[test_channel_3] = StubUpdateHandler()  # type: ignore

    config_update = ConfigUpdate(
        CommandType.REMOVE,
        (Channel(None, EpicsProtocol.NONE, None, "f?42"),),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert (
        len(update_handlers) == 3
    ), "Expected no channels to be removed as ? is not treated as a wildcard when matching schemas"


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
        CommandType.ADD,
        (
            test_channel_1,
            test_channel_2,
            test_channel_3,
        ),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert len(update_handlers) == 3
    assert test_channel_1 in update_handlers.keys()
    assert test_channel_2 in update_handlers.keys()
    assert test_channel_3 in update_handlers.keys()


def test_identical_configurations_are_not_added(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()
    channel_name = "test_channel"
    test_channel_1 = Channel(channel_name, EpicsProtocol.FAKE, "output_topic", "f142")
    test_channel_2 = Channel(channel_name, EpicsProtocol.FAKE, "output_topic", "f142")
    test_channel_3 = Channel(channel_name, EpicsProtocol.FAKE, "output_topic", "f142")
    config_update = ConfigUpdate(
        CommandType.ADD,
        (
            test_channel_1,
            test_channel_2,
            test_channel_3,
        ),
    )

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter)  # type: ignore
    assert (
        len(update_handlers) == 1
    ), "Only expect one channel to be added as others requested were identical"
    assert test_channel_1 in update_handlers.keys()


def test_configuration_stored_when_channels_added(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()
    channel_name = "test_channel"
    test_channel_1 = Channel(channel_name, EpicsProtocol.FAKE, "output_topic", "f142")
    test_channel_2 = Channel(channel_name, EpicsProtocol.FAKE, "output_topic", "f142")
    test_channel_3 = Channel(channel_name, EpicsProtocol.FAKE, "output_topic", "f142")
    config_update = ConfigUpdate(
        CommandType.ADD,
        (
            test_channel_1,
            test_channel_2,
            test_channel_3,
        ),
    )
    config_store = mock.create_autospec(ConfigurationStore)

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter, config_store)  # type: ignore

    config_store.save_configuration.assert_called_once()


def test_configuration_stored_when_channels_removed(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()
    test_channel_1 = Channel("test_channel", EpicsProtocol.FAKE, "output_topic", "f142")
    update_handlers[test_channel_1] = StubUpdateHandler()  # type: ignore
    config_update = ConfigUpdate(
        CommandType.REMOVE,
        (test_channel_1,),
    )

    config_store = mock.create_autospec(ConfigurationStore)

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter, config_store)  # type: ignore

    config_store.save_configuration.assert_called_once()


def test_configuration_stored_when_all_channels_removed(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()
    test_channel_1 = Channel("test_channel", EpicsProtocol.FAKE, "output_topic", "f142")
    update_handlers[test_channel_1] = StubUpdateHandler()  # type: ignore
    config_update = ConfigUpdate(
        CommandType.REMOVE_ALL,
        None,
    )

    config_store = mock.create_autospec(ConfigurationStore)

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter, config_store)  # type: ignore

    config_store.save_configuration.assert_called_once()


def test_configuration_not_stored_when_command_is_invalid(
    update_handlers,
):
    status_reporter = StubStatusReporter()
    producer = FakeProducer()
    test_channel_1 = Channel("test_channel", EpicsProtocol.FAKE, "output_topic", "f142")
    update_handlers[test_channel_1] = StubUpdateHandler()  # type: ignore
    config_update = ConfigUpdate(
        CommandType.INVALID,
        None,
    )

    config_store = mock.create_autospec(ConfigurationStore)

    handle_configuration_change(config_update, 20000, None, update_handlers, producer, None, None, _logger, status_reporter, config_store)  # type: ignore

    config_store.save_configuration.assert_not_called()
