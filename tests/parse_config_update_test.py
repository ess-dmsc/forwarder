from unittest.mock import patch

from flatbuffers.packer import struct as flatbuffer_struct
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.Protocol import (
    Protocol,
)
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)
from streaming_data_types.forwarder_config_update_rf5k import (
    StreamInfo,
    deserialise_rf5k,
    serialise_rf5k,
)

from forwarder.parse_config_update import (
    CommandType,
    _parse_streams,
    parse_config_update,
)


def test_parsing_returns_as_invalid_for_message_which_is_not_valid_rf5k_flatbuffer():
    message = b"something_which_is_not_a_valid_rf5k_flatbuffer"
    config_update = parse_config_update(message)
    assert config_update.command_type == CommandType.INVALID


def test_parses_removeall_config_type():
    message = serialise_rf5k(UpdateType.REMOVEALL, [])
    config_update = parse_config_update(message)
    assert config_update.command_type == CommandType.REMOVE_ALL


def test_remove_config_type_with_no_streams_is_invalid():
    message = serialise_rf5k(UpdateType.REMOVE, [])
    config_update = parse_config_update(message)
    assert config_update.command_type == CommandType.INVALID


def test_add_config_type_with_no_streams_is_invalid():
    message = serialise_rf5k(UpdateType.ADD, [])
    config_update = parse_config_update(message)
    assert config_update.command_type == CommandType.INVALID


def test_parse_streams_skips_stream_info_if_add_config_and_channel_not_specified():
    empty_channel = ""
    message = serialise_rf5k(
        UpdateType.ADD,
        [StreamInfo(empty_channel, "f142", "output_topic", Protocol.PVA)],
    )
    config_message = deserialise_rf5k(message)
    streams = tuple(_parse_streams(CommandType.ADD, config_message.streams))
    assert not streams


def test_parse_streams_skips_stream_info_if_add_config_and_schema_not_specified():
    empty_schema = ""
    message = serialise_rf5k(
        UpdateType.ADD,
        [StreamInfo("test_channel", empty_schema, "output_topic", Protocol.PVA)],
    )
    config_message = deserialise_rf5k(message)
    streams = tuple(_parse_streams(CommandType.ADD, config_message.streams))
    assert not streams


def test_parse_streams_skips_stream_info_if_add_config_and_topic_not_specified():
    empty_topic = ""
    message = serialise_rf5k(
        UpdateType.ADD,
        [StreamInfo("test_channel", "f142", empty_topic, Protocol.PVA)],
    )
    config_message = deserialise_rf5k(message)
    streams = tuple(_parse_streams(CommandType.ADD, config_message.streams))
    assert not streams


def test_parse_streams_skips_stream_info_if_add_config_and_schema_not_recognised():
    nonexistent_schema = "NONEXISTENT"
    message = serialise_rf5k(
        UpdateType.ADD,
        [StreamInfo("test_channel", nonexistent_schema, "output_topic", Protocol.PVA)],
    )
    config_message = deserialise_rf5k(message)
    streams = tuple(_parse_streams(CommandType.ADD, config_message.streams))
    assert not streams


def test_parse_streams_parses_valid_add_config():
    test_channel_name = "test_channel"
    message = serialise_rf5k(
        UpdateType.ADD,
        [StreamInfo(test_channel_name, "f142", "output_topic", Protocol.PVA)],
    )
    config_message = deserialise_rf5k(message)
    streams = tuple(_parse_streams(CommandType.ADD, config_message.streams))
    assert len(streams) == 1
    assert streams[0].name == test_channel_name


def test_parse_streams_parses_valid_stream_after_skipping_invalid_stream():
    nonexistent_schema = "NONEXISTENT"
    valid_stream_channel_name = "test_valid_stream"
    message = serialise_rf5k(
        UpdateType.ADD,
        [
            StreamInfo(
                "test_invalid_stream", nonexistent_schema, "output_topic", Protocol.PVA
            ),
            StreamInfo(valid_stream_channel_name, "f142", "output_topic", Protocol.PVA),
        ],
    )
    config_message = deserialise_rf5k(message)
    streams = tuple(_parse_streams(CommandType.ADD, config_message.streams))
    assert len(streams) == 1
    assert streams[0].name == valid_stream_channel_name


def test_remove_config_is_valid_with_all_channel_info_specified():
    test_channel_name = "test_channel"
    message = serialise_rf5k(
        UpdateType.REMOVE,
        [StreamInfo(test_channel_name, "f142", "output_topic", Protocol.PVA)],
    )
    config_message = deserialise_rf5k(message)
    streams = tuple(_parse_streams(CommandType.REMOVE, config_message.streams))
    assert len(streams) == 1
    assert streams[0].name == test_channel_name


def test_remove_config_is_valid_if_channel_name_not_specified_but_schema_is():
    test_schema = "f142"
    message = serialise_rf5k(
        UpdateType.REMOVE,
        [StreamInfo("", test_schema, "", Protocol.PVA)],
    )
    config_message = deserialise_rf5k(message)
    streams = tuple(_parse_streams(CommandType.REMOVE, config_message.streams))
    assert len(streams) == 1
    assert streams[0].schema == test_schema


def test_remove_config_is_valid_if_channel_name_not_specified_but_topic_is():
    test_topic = "output_topic"
    message = serialise_rf5k(
        UpdateType.REMOVE,
        [StreamInfo("", "", test_topic, Protocol.PVA)],
    )
    config_message = deserialise_rf5k(message)
    streams = tuple(_parse_streams(CommandType.REMOVE, config_message.streams))
    assert len(streams) == 1
    assert streams[0].output_topic == test_topic


def test_parse_streams_skips_stream_info_if_remove_config_and_channel_name_schema_and_topic_not_specified():
    message = serialise_rf5k(
        UpdateType.REMOVE,
        [StreamInfo("", "", "", Protocol.PVA)],
    )
    config_message = deserialise_rf5k(message)
    streams = tuple(_parse_streams(CommandType.REMOVE, config_message.streams))
    assert not streams


def test_parse_streams_skips_stream_info_if_remove_config_and_schema_present_but_not_recognised():
    nonexistent_schema = "NONEXISTENT"
    message = serialise_rf5k(
        UpdateType.REMOVE,
        [StreamInfo("test_channel", nonexistent_schema, "output_topic", Protocol.PVA)],
    )
    config_message = deserialise_rf5k(message)
    streams = tuple(_parse_streams(CommandType.REMOVE, config_message.streams))
    assert not streams


@patch(
    "forwarder.parse_config_update.deserialise_rf5k",
    side_effect=RuntimeError("Runtime Error"),
)
def test_command_is_invalid_on_runtime_error(mock_func):
    message = serialise_rf5k(
        UpdateType.ADD,
        [StreamInfo("test_channel", "f142", "output_topic", Protocol.PVA)],
    )
    config_update = parse_config_update(message)
    assert config_update.command_type == CommandType.INVALID


@patch(
    "forwarder.parse_config_update.deserialise_rf5k",
    side_effect=flatbuffer_struct.error("Flatbuffer Error"),
)
def test_command_is_invalid_on_flatbuffer_struct_error(mock_func):
    message = serialise_rf5k(
        UpdateType.ADD,
        [StreamInfo("test_channel", "f142", "output_topic", Protocol.PVA)],
    )
    config_update = parse_config_update(message)
    assert config_update.command_type == CommandType.INVALID


@patch(
    "forwarder.parse_config_update.deserialise_rf5k",
    side_effect=WrongSchemaException("Wrong Schema Exception"),
)
def test_command_is_invalid_on_wrong_schema_exception(mock_func):
    message = serialise_rf5k(
        UpdateType.ADD,
        [StreamInfo("test_channel", "f142", "output_topic", Protocol.PVA)],
    )
    config_update = parse_config_update(message)
    assert config_update.command_type == CommandType.INVALID


def test_command_is_invalid_if_invalid_update_type_is_specified():
    invalid_update_type = 9999
    message = serialise_rf5k(
        invalid_update_type,
        [StreamInfo("test_channel", "f142", "output_topic", Protocol.PVA)],
    )
    config_update = parse_config_update(message)
    assert config_update.command_type == CommandType.INVALID


def test_parse_streams_skips_stream_info_if_invalid_protocol_is_specified():
    invalid_protocol = 9999
    message = serialise_rf5k(
        UpdateType.REMOVE,
        [StreamInfo("test_channel", "f142", "output_topic", invalid_protocol)],
    )
    config_message = deserialise_rf5k(message)
    streams = tuple(_parse_streams(CommandType.REMOVE, config_message.streams))
    assert not streams
