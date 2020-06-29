from confluent_kafka import TopicPartition, Consumer
from .helpers.producerwrapper import ProducerWrapper
from .helpers.forwarderconfig import EpicsProtocol
from time import sleep
from .helpers.flatbuffer_helpers import (
    check_expected_value,
    check_multiple_expected_values,
    check_expected_alarm_status,
)
from .helpers.kafka_helpers import (
    create_consumer,
    poll_for_valid_message,
    get_last_available_status_message,
)
from .helpers.epics_helpers import change_pv_value
from .helpers.PVs import (
    PVDOUBLE,
    PVSTR,
    PVLONG,
    PVENUM,
    PVFLOATARRAY,
    PVDOUBLE_WITH_ALARM_THRESHOLDS,
)
import json
import numpy as np
from .helpers.f142_logdata.AlarmSeverity import AlarmSeverity
from .helpers.f142_logdata.AlarmStatus import AlarmStatus
import pytest

CONFIG_TOPIC = "TEST_forwarderConfig"
INITIAL_STRING_VALUE = "test"
INITIAL_ENUM_VALUE = "INIT"
INITIAL_LONG_VALUE = 0
INITIAL_DOUBLE_VALUE = 0.0
INITIAL_FLOATARRAY_VALUE = (1.1, 2.2, 3.3, 4.4, 5.5)


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
        PVSTR: INITIAL_STRING_VALUE,
        PVLONG: INITIAL_LONG_VALUE,
        PVENUM: np.array([INITIAL_ENUM_VALUE]).astype(np.string_),
        PVDOUBLE_WITH_ALARM_THRESHOLDS: INITIAL_DOUBLE_VALUE,
        PVFLOATARRAY: INITIAL_FLOATARRAY_VALUE,
    }

    for key, value in initial_values.items():
        change_pv_value(key, value)

    sleep(1)

    # TEARDOWN
    # Everything after yield is executed after the test function
    yield
    print("Resetting PVs", flush=True)
    prod = ProducerWrapper("localhost:9092", CONFIG_TOPIC, "")
    prod.stop_all_pvs()

    sleep(2)


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


@pytest.mark.parametrize("epics_protocol", [EpicsProtocol.CA, EpicsProtocol.PVA])
def test_forwarding_of_various_pv_types(epics_protocol, docker_compose_forwarding):
    # Update forwarder configuration over Kafka
    # The SoftIOC makes our test PVs available over CA and PVA, so we can test both here

    # Use a different topic for each parameter value, otherwise failing one test can cause the following tests to fail
    data_topic = f"TEST_forwarderData_{epics_protocol.value}"

    wait_for_forwarder_to_be_ready()
    prod = ProducerWrapper(
        "localhost:9092", CONFIG_TOPIC, data_topic, epics_protocol=epics_protocol
    )
    cons = create_consumer()
    cons.subscribe([data_topic])

    forwarding_enum(cons, prod)
    consumer_seek_to_end_of_topic(cons, data_topic)
    forwarding_floatarray(cons, prod)
    consumer_seek_to_end_of_topic(cons, data_topic)
    forwarding_string_and_long(cons, prod)
    consumer_seek_to_end_of_topic(cons, data_topic)
    forwarding_double_with_alarm(cons, prod)

    cons.close()


def consumer_seek_to_end_of_topic(consumer: Consumer, data_topic: str):
    consumer.unsubscribe()
    sleep(1)
    # Resubscribe at end of topic
    consumer.subscribe([data_topic])


def forwarding_double_with_alarm(consumer: Consumer, producer: ProducerWrapper):
    pvs = [PVDOUBLE_WITH_ALARM_THRESHOLDS]
    producer.add_config(pvs)
    # Wait for config change to be picked up
    sleep(5)

    # Change the PV value, so something is forwarded
    # New value is between the HIGH and HIHI alarm thresholds
    first_updated_value = 17
    change_pv_value(PVDOUBLE_WITH_ALARM_THRESHOLDS, first_updated_value)

    # Change the PV value, so something is forwarded
    # New value is still between the HIGH and HIHI alarm thresholds
    second_updated_value = 18
    change_pv_value(PVDOUBLE_WITH_ALARM_THRESHOLDS, second_updated_value)

    # Wait for PV to be updated
    sleep(5)
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
    check_expected_alarm_status(
        third_msg, AlarmStatus.NO_CHANGE, AlarmSeverity.NO_CHANGE
    )


def forwarding_enum(consumer: Consumer, producer: ProducerWrapper):
    pvs = [PVENUM]
    producer.add_config(pvs)
    # Wait for config change to be picked up
    sleep(5)
    new_enum_value = "START"
    change_pv_value(PVENUM, np.array([new_enum_value]).astype(np.string_))
    # Wait for forwarder to forward PV update into Kafka
    sleep(5)
    first_msg, _ = poll_for_valid_message(consumer)
    check_expected_value(first_msg, PVENUM, INITIAL_ENUM_VALUE)
    second_msg, _ = poll_for_valid_message(consumer)
    check_expected_value(second_msg, PVENUM, new_enum_value)
    producer.remove_config(pvs)


def forwarding_floatarray(consumer: Consumer, producer: ProducerWrapper):
    pvs = [PVFLOATARRAY]
    producer.add_config(pvs)
    # Wait for config to be pushed
    sleep(5)
    new_floatarray_value = [0.0, 0.1, 0.2, 0.3, 0.4]
    change_pv_value(PVFLOATARRAY, np.array(new_floatarray_value).astype(np.string_))
    # Wait for forwarder to forward PV update into Kafka
    sleep(5)
    first_msg, _ = poll_for_valid_message(consumer)
    check_expected_value(first_msg, PVFLOATARRAY, INITIAL_FLOATARRAY_VALUE)
    second_msg, _ = poll_for_valid_message(consumer)
    check_expected_value(second_msg, PVFLOATARRAY, new_floatarray_value)
    producer.remove_config(pvs)


def forwarding_string_and_long(consumer: Consumer, producer: ProducerWrapper):
    pvs = [PVSTR, PVLONG]
    producer.add_config(pvs)
    # Wait for config to be pushed
    sleep(5)
    initial_string_value = INITIAL_STRING_VALUE
    initial_long_value = INITIAL_LONG_VALUE
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
    pvs = [PVSTR, PVLONG]
    prod = ProducerWrapper("localhost:9092", CONFIG_TOPIC, data_topic)
    prod.add_config(pvs)

    sleep(2)
    cons.subscribe([status_topic])
    sleep(5)

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


def test_forwarder_can_handle_rapid_config_updates(docker_compose_forwarding):
    cons = create_consumer("latest")
    wait_for_forwarder_to_be_ready()

    status_topic = "TEST_forwarderStatus"
    data_topic = "TEST_forwarderData_connection_status"

    prod = ProducerWrapper(
        "localhost:9092", CONFIG_TOPIC, data_topic, epics_protocol=EpicsProtocol.PVA
    )
    configured_list_of_pvs = []
    number_of_config_updates = 100
    for i in range(number_of_config_updates):
        pv = f"{PVDOUBLE}{i}"
        prod.add_config([pv])
        configured_list_of_pvs.append(pv)

    sleep(5)
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


def test_forwarder_sends_fake_pv_updates(docker_compose_forwarding):
    data_topic = "TEST_forwarderData_fake"
    wait_for_forwarder_to_be_ready()
    producer = ProducerWrapper(
        "localhost:9092", CONFIG_TOPIC, data_topic, epics_protocol=EpicsProtocol.FAKE
    )
    producer.add_config(["FakePV"])

    # A fake PV is defined in the config json file with channel name "FakePV"
    consumer = create_consumer()
    consumer.subscribe([data_topic])
    sleep(5)
    msg, _ = poll_for_valid_message(consumer)
    # We should see PV updates in Kafka despite there being no IOC running
    check_expected_value(msg, "FakePV", None)
