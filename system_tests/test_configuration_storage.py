from .helpers.producerwrapper import ProducerWrapper
from time import sleep
from .helpers.kafka_helpers import create_consumer, poll_for_valid_message
from .helpers.PVs import PVSTR, PVLONG
import json
from streaming_data_types.status_x5f2 import deserialise_x5f2

CONFIG_TOPIC = "TEST_forwarderConfig"


def test_forwarder_storage_writes_added_pvs(docker_compose_storage):
    """
    WHEN A message configures two new PVs to be forwarded
    THEN storage message lists new PVs
    """
    data_topic = "TEST_forwarderData_change_config"
    storage_topic = "TEST_forwarderStorage"
    cons = create_consumer()
    cons.subscribe([storage_topic])
    sleep(5)

    pvs = [PVSTR, PVLONG]
    prod = ProducerWrapper("localhost:9092", CONFIG_TOPIC, data_topic)
    prod.add_config(pvs)

    sleep(5)

    storage_msg, _ = poll_for_valid_message(cons, expected_file_identifier=None)

    storage_msg = deserialise_x5f2(storage_msg)
    status_json = json.loads(storage_msg.status_json)
    names_of_channels_being_forwarded = {stream["channel"] for stream in status_json}
    names_newly_forwarded_pvs = {PVSTR, PVLONG}

    assert (
        names_newly_forwarded_pvs
        == names_newly_forwarded_pvs & names_of_channels_being_forwarded
    ), (
        f"Expect these channels to be configured as forwarded: {names_newly_forwarded_pvs}, "
        f"but storage message report only has: {names_of_channels_being_forwarded}"
    )

    cons.close()


def test_forwarder_storage_shows_added_pvs():
    # TODO:
    pass
