import logging

import pytest

from forwarder.common import Channel, EpicsProtocol
from forwarder.update_handlers.ca_update_handler import CAUpdateHandler
from forwarder.update_handlers.create_update_handler import create_update_handler
from forwarder.update_handlers.pva_update_handler import PVAUpdateHandler
from tests.kafka.fake_producer import FakeProducer
from tests.test_helpers.ca_fakes import FakeContext as FakeCAContext
from tests.test_helpers.p4p_fakes import FakeContext as FakePVAContext

_logger = logging.getLogger("stub_for_use_in_tests")
_logger.addHandler(logging.NullHandler())


def test_create_update_handler_throws_if_channel_has_no_name():
    producer = FakeProducer()
    channel_with_no_name = Channel(None, EpicsProtocol.PVA, "output_topic", "f142", 0)
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_no_name, 20000)  # type: ignore

    channel_with_empty_name = Channel("", EpicsProtocol.PVA, "output_topic", "f142", 0)
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_empty_name, 20000)  # type: ignore


def test_create_update_handler_throws_if_channel_has_no_topic():
    producer = FakeProducer()
    channel_with_no_topic = Channel("name", EpicsProtocol.PVA, None, "f142", 0)
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_no_topic, 20000)  # type: ignore

    channel_with_empty_topic = Channel("name", EpicsProtocol.PVA, "", "f142", 0)
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_empty_topic, 20000)  # type: ignore


def test_create_update_handler_throws_if_channel_has_no_schema():
    producer = FakeProducer()
    channel_with_no_topic = Channel("name", EpicsProtocol.PVA, "output_topic", None, 0)
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_no_topic, 20000)  # type: ignore

    channel_with_empty_topic = Channel("name", EpicsProtocol.PVA, "output_topic", "", 0)
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_empty_topic, 20000)  # type: ignore


def test_create_update_handler_throws_if_protocol_not_specified():
    producer = FakeProducer()
    channel_with_no_protocol = Channel(
        "name", EpicsProtocol.NONE, "output_topic", "f142", 0
    )
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_no_protocol, 20000)  # type: ignore


def test_create_update_handler_throws_if_periodic_not_specified():
    producer = FakeProducer()
    channel_with_no_periodic = Channel(
        "name", EpicsProtocol.PVA, "output_topic", "f142", None
    )
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_no_periodic, 20000)  # type: ignore


def test_pva_handler_created_when_pva_protocol_specified():
    producer = FakeProducer()
    context = FakePVAContext()
    channel_with_pva_protocol = Channel(
        "name", EpicsProtocol.PVA, "output_topic", "f142", 0
    )
    handler = create_update_handler(producer, None, context, channel_with_pva_protocol, 20000)  # type: ignore
    assert isinstance(handler, PVAUpdateHandler)
    handler.stop()


def test_ca_handler_created_when_ca_protocol_specified():
    producer = FakeProducer()
    context = FakeCAContext()
    channel_with_ca_protocol = Channel(
        "name", EpicsProtocol.CA, "output_topic", "f142", 0
    )
    handler = create_update_handler(producer, context, None, channel_with_ca_protocol, 20000)  # type: ignore
    assert isinstance(handler, CAUpdateHandler)
    handler.stop()
