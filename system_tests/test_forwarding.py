import json
from time import sleep

import numpy as np
import pytest
from caproto._utils import CaprotoTimeoutError
from confluent_kafka import Consumer, TopicPartition
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.Protocol import (
    Protocol,
)
from streaming_data_types.status_x5f2 import deserialise_x5f2

from .helpers.epics_helpers import change_pv_value
from .helpers.f142_logdata.AlarmSeverity import AlarmSeverity
from .helpers.f142_logdata.AlarmStatus import AlarmStatus
from .helpers.flatbuffer_helpers import (
    check_expected_alarm_status,
    check_expected_value,
    check_multiple_expected_values,
)
from .helpers.kafka_helpers import (
    create_consumer,
    get_last_available_status_message,
    poll_for_valid_message,
)
from .helpers.producerwrapper import ProducerWrapper
from .helpers.PVs import (
    PVDOUBLE,
    PVDOUBLE_WITH_ALARM_THRESHOLDS,
    PVFLOATARRAY,
    PVLONG,
    PVSTR,
)

CONFIG_TOPIC = "TEST_forwarderConfig"
INITIAL_LONG_VALUE = 0
INITIAL_DOUBLE_VALUE = 0.0
INITIAL_FLOATARRAY_VALUE = (1.1, 2.2, 3.3, 4.4, 5.5)
SLEEP_TIME = 3


@pytest.fixture(scope="function", autouse=True)
def setup_and_teardown_function(request):
    """
    Stops forwarder pv listening and resets any values in EPICS
    """
    # SETUP
    initial_values = {
        PVDOUBLE: INITIAL_DOUBLE_VALUE,
        # We have to use this as the second parameter for caput gets parsed as empty so does not change the value of
        # the PV
        PVLONG: INITIAL_LONG_VALUE,
        PVDOUBLE_WITH_ALARM_THRESHOLDS: INITIAL_DOUBLE_VALUE,
        PVFLOATARRAY: INITIAL_FLOATARRAY_VALUE,
    }

    for key, value in initial_values.items():
        pv_set = False
        attempts = 0
        while not pv_set and attempts < 10:
            try:
                change_pv_value(key, value, Protocol.CA)
                change_pv_value(key, value, Protocol.PVA)
            except CaprotoTimeoutError:
                attempts += 1
                print(f"Failed to write to {key}. Retrying.")
                continue
            pv_set = True

    sleep(SLEEP_TIME)

    # TEARDOWN
    # Everything after yield is executed after the test function
    yield
    print("Resetting PVs", flush=True)
    prod = ProducerWrapper("localhost:9092", CONFIG_TOPIC, "")
    prod.stop_all_pvs()

    sleep(SLEEP_TIME)


def wait_for_forwarder_to_be_ready(
    timeout_s: float = 20, status_topic: str = "TEST_forwarderStatus"
):
    """
    Wait for the Forwarder to produce a status message
    """
    cons = create_consumer("latest")
    cons.assign([TopicPartition(status_topic, partition=0)])
    cons.consume(timeout=timeout_s)
    cons.close()


@pytest.mark.parametrize("epics_protocol", [Protocol.CA, Protocol.PVA])
def test_forwarding_of_various_pv_types(epics_protocol, docker_compose_forwarding):
    # Update forwarder configuration over Kafka
    # The SoftIOC makes our test PVs available over CA and PVA, so we can test both here

    # Use a different topic for each parameter value, otherwise failing one test can cause the following tests to fail
    data_topic = f"TEST_forwarderData_{epics_protocol}"

    wait_for_forwarder_to_be_ready()
    prod = ProducerWrapper(
        "localhost:9092", CONFIG_TOPIC, data_topic, epics_protocol=epics_protocol
    )
    cons = create_consumer()
    cons.subscribe([data_topic])

    consumer_seek_to_end_of_topic(cons, data_topic)
    forwarding_floatarray(cons, prod, epics_protocol)
    consumer_seek_to_end_of_topic(cons, data_topic)
    forwarding_long(cons, prod, epics_protocol)
    consumer_seek_to_end_of_topic(cons, data_topic)
    forwarding_double_with_alarm(cons, prod, epics_protocol)

    cons.close()


def consumer_seek_to_end_of_topic(consumer: Consumer, data_topic: str):
    consumer.unsubscribe()
    sleep(SLEEP_TIME)
    # Resubscribe at end of topic
    consumer.subscribe([data_topic])


def forwarding_double_with_alarm(
    consumer: Consumer, producer: ProducerWrapper, protocol: Protocol
):
    pvs = [PVDOUBLE_WITH_ALARM_THRESHOLDS]
    producer.add_config(pvs)
    # Wait for config change to be picked up
    sleep(SLEEP_TIME)

    # Change the PV value, so something is forwarded
    # New value is between the HIGH and HIHI alarm thresholds
    first_updated_value = 0.7
    change_pv_value(PVDOUBLE_WITH_ALARM_THRESHOLDS, first_updated_value, protocol)

    # Change the PV value, so something is forwarded
    # New value is still between the HIGH and HIHI alarm thresholds
    second_updated_value = 0.8
    change_pv_value(PVDOUBLE_WITH_ALARM_THRESHOLDS, second_updated_value, protocol)

    # Wait for PV to be updated
    sleep(SLEEP_TIME)
    # Check the initial value is forwarded
    first_msg, msg_key = poll_for_valid_message(consumer)
    check_expected_value(
        first_msg, PVDOUBLE_WITH_ALARM_THRESHOLDS, INITIAL_DOUBLE_VALUE
    )
    # Don't check alarm status of initial state because it is undefined if SoftIOC has not been changed yet
    assert msg_key == PVDOUBLE_WITH_ALARM_THRESHOLDS.encode(
        "utf-8"
    ), "Message key expected to be the same as the PV name"
    # We set the message key to be the PV name so that all messages from the same PV are sent to
    # the same partition by Kafka. This ensures that the order of these messages is maintained to the consumer.

    # Check the new value is forwarded
    second_msg, _ = poll_for_valid_message(consumer)
    check_expected_value(
        second_msg, PVDOUBLE_WITH_ALARM_THRESHOLDS, first_updated_value
    )
    check_expected_alarm_status(second_msg, AlarmStatus.HIGH, AlarmSeverity.MINOR)

    # Check the new value is forwarded, but alarm status should be unchanged
    third_msg, _ = poll_for_valid_message(consumer)
    check_expected_value(
        third_msg, PVDOUBLE_WITH_ALARM_THRESHOLDS, second_updated_value
    )
    check_expected_alarm_status(third_msg, AlarmStatus.HIGH, AlarmSeverity.MINOR)


def forwarding_floatarray(
    consumer: Consumer, producer: ProducerWrapper, protocol: Protocol
):
    pvs = [PVFLOATARRAY]
    producer.add_config(pvs)
    # Wait for config to be pushed
    sleep(SLEEP_TIME)
    new_floatarray_value = [0.0, 0.1, 0.2, 0.3, 0.4]
    change_pv_value(PVFLOATARRAY, np.array(new_floatarray_value), protocol)
    # Wait for forwarder to forward PV update into Kafka
    sleep(SLEEP_TIME)
    first_msg, _ = poll_for_valid_message(consumer)
    check_expected_value(first_msg, PVFLOATARRAY, INITIAL_FLOATARRAY_VALUE)
    second_msg, _ = poll_for_valid_message(consumer)
    check_expected_value(second_msg, PVFLOATARRAY, new_floatarray_value)
    producer.remove_config(pvs)


def forwarding_long(consumer: Consumer, producer: ProducerWrapper, protocol: Protocol):
    pvs = [
        PVLONG,
    ]
    producer.add_config(pvs)
    # Wait for config to be pushed
    sleep(SLEEP_TIME)
    initial_long_value = INITIAL_LONG_VALUE
    # Wait for forwarder to forward PV update into Kafka
    sleep(SLEEP_TIME)
    expected_values = {
        PVLONG: initial_long_value,
    }
    first_msg, _ = poll_for_valid_message(consumer)
    messages = [
        first_msg,
    ]
    check_multiple_expected_values(messages, expected_values)
    producer.remove_config(pvs)


def test_forwarder_status_shows_added_pvs(docker_compose_forwarding):
    """
    GIVEN A PV (double type) is already being forwarded
    WHEN A message configures two additional PV (str and long types) to be forwarded
    THEN Forwarder status message lists new PVs
    """
    cons = create_consumer("latest")
    wait_for_forwarder_to_be_ready()

    data_topic = "TEST_forwarderData_change_config"
    status_topic = "TEST_forwarderStatus"
    # Send first command
    prod = ProducerWrapper("localhost:9092", CONFIG_TOPIC, data_topic)
    prod.add_config([PVSTR, PVLONG])
    # Send second command with PVLONG channel to different topic
    prod = ProducerWrapper("localhost:9092", CONFIG_TOPIC, "some_topic", Protocol.PVA)
    prod.add_config([PVLONG])

    sleep(SLEEP_TIME)
    cons.subscribe([status_topic])
    sleep(SLEEP_TIME)

    # Poll for the latest message
    status_msg, _ = poll_for_valid_message(cons, expected_file_identifier=None)

    status_msg = deserialise_x5f2(status_msg)
    status_json = json.loads(status_msg.status_json)

    info_of_channels_being_forwarded = {
        (
            stream["channel_name"],
            stream["protocol"],
            stream["output_topic"],
            stream["schema"],
        )
        for stream in status_json["streams"]
    }
    expected_info_of_channels_being_forwarded = {
        (PVSTR, "CA", data_topic, "f142"),
        (PVLONG, "CA", data_topic, "f142"),
        (PVLONG, "PVA", "some_topic", "f142"),
    }
    assert (
        expected_info_of_channels_being_forwarded == info_of_channels_being_forwarded
    ), (
        f"Expect these channels to be configured as forwarded: "
        f"{expected_info_of_channels_being_forwarded}, "
        f"but status message report these as forwarded: "
        f"{info_of_channels_being_forwarded}"
    )

    cons.close()


def test_forwarder_can_handle_rapid_config_updates(docker_compose_forwarding):
    cons = create_consumer("latest")
    wait_for_forwarder_to_be_ready()

    status_topic = "TEST_forwarderStatus"
    data_topic = "TEST_forwarderData_connection_status"

    prod = ProducerWrapper(
        "localhost:9092", CONFIG_TOPIC, data_topic, epics_protocol=Protocol.PVA
    )
    configured_list_of_pvs = []
    number_of_config_updates = 100
    for i in range(number_of_config_updates):
        pv = f"{PVDOUBLE}{i}"
        prod.add_config([pv])
        configured_list_of_pvs.append(pv)

    sleep(SLEEP_TIME)
    cons.assign([TopicPartition(status_topic, partition=0)])
    sleep(SLEEP_TIME)
    # Get the last available status message
    status_msg = get_last_available_status_message(cons, status_topic)

    status_msg = deserialise_x5f2(status_msg)
    status_json = json.loads(status_msg.status_json)
    streams_json = status_json["streams"]
    streams = []
    for item in streams_json:
        streams.append(item["channel_name"])

    for pv in configured_list_of_pvs:
        assert pv in streams, "Expect configured PV to be reported as being forwarded"


def test_forwarder_sends_fake_pv_updates(docker_compose_forwarding):
    data_topic = "TEST_forwarderData_fake"
    wait_for_forwarder_to_be_ready()
    producer = ProducerWrapper(
        "localhost:9092", CONFIG_TOPIC, data_topic, epics_protocol=Protocol.FAKE
    )
    producer.add_config(["FakePV"])

    # A fake PV is defined in the config json file with channel name "FakePV"
    consumer = create_consumer()
    consumer.subscribe([data_topic])
    sleep(SLEEP_TIME)
    msg, _ = poll_for_valid_message(consumer)
    # We should see PV updates in Kafka despite there being no IOC running
    check_expected_value(msg, "FakePV", None)
