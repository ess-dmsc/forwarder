import json
from unittest import mock
from confluent_kafka import Consumer
from streaming_data_types import deserialise_x5f2, serialise_x5f2

from forwarder.configuration_store import ConfigurationStore
from forwarder.parse_config_update import Channel, EpicsProtocol
from tests.kafka.fake_producer import FakeProducer


CHANNELS_TO_STORE = {
    "channel1": Channel("channel1", EpicsProtocol.PVA, "topic1", "f142"),
    "channel2": Channel("channel2", EpicsProtocol.PVA, "topic2", "tdct"),
}

EMPTY_STORED_JSON = "[]"

NONEMPTY_STORED_JSON = json.dumps(
    [
        {"channel": "channel1", "converter": {"topic": "topic1", "schema": "f142"}},
        {"channel": "channel2", "converter": {"topic": "topic2", "schema": "tdct"}},
    ]
)


class FakeKafkaMessage:
    def __init__(self, message):
        self._message = message

    def value(self):
        return self._message


def assert_stored_channel_correct(outputted_channel):
    assert outputted_channel["channel"] in CHANNELS_TO_STORE
    original = CHANNELS_TO_STORE[outputted_channel["channel"]]
    assert outputted_channel["converter"]["topic"] == original.output_topic
    assert outputted_channel["converter"]["schema"] == original.schema


def test_when_multiple_pvs_dumped_config_contains_all_pv_details():
    producer = FakeProducer()
    store = ConfigurationStore(producer, consumer=None, topic="store_topic")

    store.save_configuration(CHANNELS_TO_STORE)

    stored_message = deserialise_x5f2(producer.published_payload)
    stored_channels = json.loads(stored_message.status_json)  # type: ignore

    assert_stored_channel_correct(stored_channels[0])
    assert_stored_channel_correct(stored_channels[1])


def test_when_no_pvs_stored_info_contains_no_pvs():
    producer = FakeProducer()
    store = ConfigurationStore(producer, consumer=None, topic="store_topic")

    store.save_configuration({})

    stored_message = deserialise_x5f2(producer.published_payload)
    stored_channels = json.loads(stored_message.status_json)  # type: ignore

    assert len(stored_channels) == 0


def test_retrieving_stored_info_with_no_pvs_gets_empty_streams():
    mock_consumer = mock.create_autospec(Consumer)
    mock_consumer.get_watermark_offsets.return_value = (0, 100)
    message = serialise_x5f2("", "", "", "", 0, 0, status_json=EMPTY_STORED_JSON)
    mock_consumer.consume.return_value = [FakeKafkaMessage(message)]
    store = ConfigurationStore(
        producer=None, consumer=mock_consumer, topic="store_topic"
    )

    config_as_json = store.retrieve_configuration()
    config = json.loads(config_as_json)

    assert len(config["streams"]) == 0


def test_retrieving_stored_info_with_multiple_pvs_gets_streams():
    mock_consumer = mock.create_autospec(Consumer)
    mock_consumer.get_watermark_offsets.return_value = (0, 100)
    message = serialise_x5f2("", "", "", "", 0, 0, status_json=NONEMPTY_STORED_JSON)
    mock_consumer.consume.return_value = [FakeKafkaMessage(message)]
    store = ConfigurationStore(
        producer=None, consumer=mock_consumer, topic="store_topic"
    )

    config_as_json = store.retrieve_configuration()
    config = json.loads(config_as_json)

    assert len(config["streams"]) == 2
    assert_stored_channel_correct(config["streams"][0])
    assert_stored_channel_correct(config["streams"][1])
