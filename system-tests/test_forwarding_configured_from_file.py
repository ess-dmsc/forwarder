from helpers.f142_logdata.Value import Value
from time import sleep
from helpers.kafka_helpers import create_consumer, poll_for_valid_message
from helpers.flatbuffer_helpers import check_expected_value, check_expected_alarm_status
from helpers.epics_helpers import change_pv_value
from helpers.PVs import PVDOUBLE_WITH_ALARM_THRESHOLDS
from helpers.f142_logdata.AlarmSeverity import AlarmSeverity
from helpers.f142_logdata.AlarmStatus import AlarmStatus

CONFIG_TOPIC = "TEST_forwarderConfig"


def test_forwarding_configured_from_file(docker_compose):
    cons = create_consumer()
    cons.subscribe(["TEST_forwarderData_pv_from_config"])
    sleep(5)

    initial_value = 0

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
    first_msg, msg_key = poll_for_valid_message(cons)
    check_expected_value(
        first_msg, Value.Double, PVDOUBLE_WITH_ALARM_THRESHOLDS, initial_value
    )
    check_expected_alarm_status(first_msg, AlarmStatus.UDF, AlarmSeverity.NO_ALARM)
    assert msg_key == PVDOUBLE_WITH_ALARM_THRESHOLDS.encode(
        "utf-8"
    ), "Message key expected to be the same as the PV name"
    # We set the message key to be the PV name so that all messages from the same PV are sent to
    # the same partition by Kafka. This ensures that the order of these messages is maintained to the consumer.

    # Check the new value is forwarded
    second_msg, _ = poll_for_valid_message(cons)
    check_expected_value(
        second_msg, Value.Double, PVDOUBLE_WITH_ALARM_THRESHOLDS, first_updated_value
    )
    check_expected_alarm_status(second_msg, AlarmStatus.HIGH, AlarmSeverity.MINOR)

    # Check the new value is forwarded, but alarm status should be unchanged
    third_msg, _ = poll_for_valid_message(cons)
    check_expected_value(
        third_msg, Value.Double, PVDOUBLE_WITH_ALARM_THRESHOLDS, second_updated_value
    )
    check_expected_alarm_status(
        third_msg, AlarmStatus.NO_CHANGE, AlarmSeverity.NO_CHANGE
    )

    cons.close()
