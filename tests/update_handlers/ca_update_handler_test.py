from tests.kafka.fake_producer import FakeProducer
from forwarder.update_handlers.ca_update_handler import CAUpdateHandler
from tests.test_helpers.ca_fakes import FakeContext
import pytest


def test_update_handler_throws_if_schema_not_recognised():
    producer = FakeProducer()
    context = FakeContext()
    non_existing_schema = "DOESNTEXIST"
    with pytest.raises(ValueError):
        CAUpdateHandler(producer, context, "source_name", "output_topic", non_existing_schema)  # type: ignore
