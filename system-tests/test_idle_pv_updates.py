from helpers.kafka_helpers import create_consumer, poll_for_valid_message
from helpers.f142_logdata.Value import Value
from helpers.f142_logdata.AlarmSeverity import AlarmSeverity
from helpers.f142_logdata.AlarmStatus import AlarmStatus
from helpers.flatbuffer_helpers import check_expected_value
from time import sleep
from helpers.PVs import PVDOUBLE


def test_forwarder_sends_idle_pv_updates(docker_compose_idle_updates):
    consumer = create_consumer()
    data_topic = "TEST_forwarderData_idle_updates"
    consumer.subscribe([data_topic])
    sleep(10)
    for i in range(3):
        msg, _ = poll_for_valid_message(consumer)
        check_expected_value(msg, Value.Double, PVDOUBLE, 0)
        assert (
            msg.Status() != AlarmStatus.NO_CHANGE
        ), "Expect logs from periodic updates to always contain the current EPICS alarm status"
        assert (
            msg.Severity() != AlarmSeverity.NO_CHANGE
        ), "Expect logs from periodic updates to always contain the current EPICS alarm severity"
    consumer.close()
