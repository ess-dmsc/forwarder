from confluent_kafka import TopicPartition, Consumer
from helpers.producerwrapper import ProducerWrapper
from time import sleep
from helpers.flatbuffer_helpers import (
    check_expected_value,
    check_multiple_expected_values,
)
from helpers.kafka_helpers import (
    create_consumer,
    poll_for_valid_message,
    get_last_available_status_message,
)
from helpers.epics_helpers import change_pv_value
from helpers.PVs import PVDOUBLE, PVSTR, PVLONG, PVENUM, PVFLOATARRAY
import json
import numpy as np
import pytest

CONFIG_TOPIC = "TEST_forwarderConfig"
INITIAL_FLOATARRAY_VALUE = (1.1, 2.2, 3.3)


def teardown_function(function):
    """
    Stops forwarder pv listening and resets any values in EPICS
    """
    print("Resetting PVs", flush=True)
    prod = ProducerWrapper("localhost:9092", CONFIG_TOPIC, "")
    prod.stop_all_pvs()

    defaults = {
        PVDOUBLE: 0.0,
        # We have to use this as the second parameter for caput gets parsed as empty so does not change the value of
        # the PV
        PVSTR: "",
        PVLONG: 0,
        PVENUM: np.array(["INIT"]).astype(np.string_),
    }

    for key, value in defaults.items():
        change_pv_value(key, value)
    change_pv_value(PVFLOATARRAY, INITIAL_FLOATARRAY_VALUE)
    sleep(3)


def test_forwarding_of_various_pv_types(docker_compose_no_command):
    # Update forwarder configuration over Kafka
    # (rather than providing it in a JSON file when the forwarder is launched)
    data_topic = "TEST_forwarderData"

    sleep(5)
    prod = ProducerWrapper("localhost:9092", CONFIG_TOPIC, data_topic)
    cons = create_consumer()
    cons.subscribe([data_topic])

    forwarding_enum(cons, prod)
    consumer_seek_to_end_of_topic(cons, data_topic)
    # forwarding_doublearray(cons, prod)
    # consumer_seek_to_end_of_topic(cons, data_topic)
    forwarding_string_and_long(cons, prod)

    cons.close()


def consumer_seek_to_end_of_topic(consumer: Consumer, data_topic: str):
    consumer.unsubscribe()
    sleep(1)
    # Resubscribe at end of topic
    consumer.subscribe([data_topic])


def forwarding_enum(consumer: Consumer, producer: ProducerWrapper):
    pvs = [PVENUM]
    producer.add_config(pvs)
    # Wait for config change to be picked up
    sleep(5)
    change_pv_value(PVENUM, np.array(["START"]).astype(np.string_))
    # Wait for forwarder to forward PV update into Kafka
    sleep(5)
    first_msg, _ = poll_for_valid_message(consumer)
    check_expected_value(first_msg, PVENUM, 0)
    second_msg, _ = poll_for_valid_message(consumer)
    check_expected_value(second_msg, PVENUM, 1)
    producer.remove_config(pvs)


def forwarding_doublearray(consumer: Consumer, producer: ProducerWrapper):
    pvs = [PVFLOATARRAY]
    producer.add_config(pvs)
    # Wait for config to be pushed
    sleep(5)
    change_pv_value(PVFLOATARRAY, np.array([0.0, 0.1, 0.2]).astype(np.string_))
    # Wait for forwarder to forward PV update into Kafka
    sleep(5)
    first_msg, _ = poll_for_valid_message(consumer)
    check_expected_value(first_msg, PVFLOATARRAY, INITIAL_FLOATARRAY_VALUE)
    producer.remove_config(pvs)


def forwarding_string_and_long(consumer: Consumer, producer: ProducerWrapper):
    pvs = [PVSTR, PVLONG]
    producer.add_config(pvs)
    # Wait for config to be pushed
    sleep(5)
    initial_string_value = "test"
    initial_long_value = 0
    # Wait for forwarder to forward PV update into Kafka
    sleep(5)
    expected_values = {
        PVSTR: initial_string_value,
        PVLONG: initial_long_value,
    }
    first_msg, _ = poll_for_valid_message(consumer)
    second_msg, _ = poll_for_valid_message(consumer)
    messages = [first_msg, second_msg]
    check_multiple_expected_values(messages, expected_values)
    producer.remove_config(pvs)


@pytest.mark.skip("Status reporting not implemented yet")
def test_forwarder_status_shows_added_pvs(docker_compose_no_command):
    """
    GIVEN A PV (double type) is already being forwarded
    WHEN A message configures two additional PV (str and long types) to be forwarded
    THEN Forwarder status message lists new PVs
    """
    data_topic = "TEST_forwarderData_change_config"
    status_topic = "TEST_forwarderStatus"
    pvs = [PVSTR, PVLONG]
    prod = ProducerWrapper("localhost:9092", CONFIG_TOPIC, data_topic)
    prod.add_config(pvs)

    sleep(5)
    cons = create_consumer()
    sleep(2)
    cons.assign([TopicPartition(status_topic, partition=0)])
    sleep(2)

    # Get the last available status message
    partitions = cons.assignment()
    _, hi = cons.get_watermark_offsets(partitions[0], cached=False, timeout=2.0)
    last_msg_offset = hi - 1
    cons.assign([TopicPartition(status_topic, partition=0, offset=last_msg_offset)])
    status_msg, _ = poll_for_valid_message(cons, expected_file_identifier=None)

    status_json = json.loads(status_msg)
    names_of_channels_being_forwarded = {
        stream["channel_name"] for stream in status_json["streams"]
    }
    expected_names_of_channels_being_forwarded = {PVSTR, PVLONG}

    assert (
        expected_names_of_channels_being_forwarded == names_of_channels_being_forwarded
    ), (
        f"Expect these channels to be configured as forwarded: {expected_names_of_channels_being_forwarded}, "
        f"but status message report these as forwarded: {names_of_channels_being_forwarded}"
    )

    cons.close()


@pytest.mark.skip("Status reporting not implemented yet")
def test_forwarder_can_handle_rapid_config_updates(docker_compose_no_command):
    status_topic = "TEST_forwarderStatus"
    data_topic = "TEST_forwarderData_connection_status"

    base_pv = PVDOUBLE
    prod = ProducerWrapper("localhost:9092", CONFIG_TOPIC, data_topic)
    configured_list_of_pvs = []
    number_of_config_updates = 100
    for i in range(number_of_config_updates):
        pv = base_pv + str(i)
        prod.add_config([pv])
        configured_list_of_pvs.append(pv)

    sleep(5)
    cons = create_consumer()
    sleep(2)
    cons.assign([TopicPartition(status_topic, partition=0)])
    sleep(2)
    # Get the last available status message
    status_msg = get_last_available_status_message(cons, status_topic)

    streams_json = json.loads(status_msg)["streams"]
    streams = []
    for item in streams_json:
        streams.append(item["channel_name"])

    for pv in configured_list_of_pvs:
        assert pv in streams, "Expect configured PV to be reported as being forwarded"
