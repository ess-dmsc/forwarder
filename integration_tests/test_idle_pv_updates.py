from time import sleep
import pytest
from streaming_data_types.logdata_f142 import deserialise_f142

from .helpers.f142_logdata.AlarmSeverity import AlarmSeverity
from .helpers.f142_logdata.AlarmStatus import AlarmStatus
from .helpers.flatbuffer_helpers import check_expected_value
from .helpers.kafka_helpers import create_consumer, poll_for_valid_message
from .helpers.producerwrapper import ProducerWrapper
from .helpers.PVs import PVDOUBLE
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.Protocol import (
    Protocol,
)

CONFIG_TOPIC = "TEST_forwarderConfig"


@pytest.mark.parametrize("epics_protocol", [Protocol.CA, Protocol.PVA])
def test_forwarder_sends_idle_pv_updates(epics_protocol, docker_compose_idle_updates):
    data_topic = "TEST_forwarderData_idle_updates"

    sleep(5)
    producer = ProducerWrapper(
        "localhost:9092", CONFIG_TOPIC, data_topic, epics_protocol=epics_protocol
    )
    producer.add_config([PVDOUBLE])

    consumer = create_consumer()
    consumer.subscribe([data_topic])
    sleep(10)
    for i in range(3):
        msg, _ = poll_for_valid_message(consumer)
        check_expected_value(msg, PVDOUBLE, 0)
        log_data = deserialise_f142(msg)
        assert (
            log_data.alarm_status != AlarmStatus.NO_CHANGE
        ), "Expect logs from periodic updates to always contain the current EPICS alarm status"
        assert (
            log_data.alarm_severity != AlarmSeverity.NO_CHANGE
        ), "Expect logs from periodic updates to always contain the current EPICS alarm severity"
    consumer.close()
    producer.stop_all_pvs()
