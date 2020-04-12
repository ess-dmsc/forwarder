from helpers.kafka_helpers import create_consumer, poll_for_valid_message
from helpers.f142_logdata import Value
from helpers.flatbuffer_helpers import check_expected_value
from time import sleep
import pytest


@pytest.mark.skip(reason="Fake EPICS updates not implemented yet in python forwarder")
def test_forwarder_sends_fake_pv_updates(docker_compose_fake_epics):
    # A fake PV is defined in the config json file with channel name "FakePV"
    consumer = create_consumer()
    data_topic = "TEST_forward_fake_generated_pvs"
    consumer.subscribe([data_topic])
    sleep(5)
    msg, _ = poll_for_valid_message(consumer)
    # We should see PV updates in Kafka despite there being no IOC running
    check_expected_value(msg, Value.Value.Double, "FakePV", None)
