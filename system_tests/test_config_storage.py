import json

from streaming_data_types.status_x5f2 import deserialise_x5f2

from .helpers.kafka_helpers import create_consumer, poll_for_valid_message
from .helpers.PVs import PVLONG, PVSTR


def test_on_starting_stored_config_is_retrieved(docker_compose_storage):
    cons = create_consumer("latest")
    status_topic = "TEST_forwarderStorageStatus"
    cons.subscribe([status_topic])

    status_msg, _ = poll_for_valid_message(cons, expected_file_identifier=None)

    status_msg = deserialise_x5f2(status_msg)
    status_json = json.loads(status_msg.status_json)
    names_of_channels_being_forwarded = {
        stream["channel_name"] for stream in status_json["streams"]
    }
    expected_names_of_channels_being_forwarded = {PVSTR, PVLONG}

    assert (
        expected_names_of_channels_being_forwarded == names_of_channels_being_forwarded
    ), (
        f"Expect these channels to be configured as forwarded: "
        f"{expected_names_of_channels_being_forwarded}, "
        f"but status message report these as forwarded: "
        f"{names_of_channels_being_forwarded}"
    )

    cons.close()
