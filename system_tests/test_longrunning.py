from helpers.epics_helpers import change_pv_value
from helpers.kafka_helpers import (
    create_consumer,
    poll_for_valid_message,
    MsgErrorException,
)
from helpers.PVs import PVDOUBLE
import pytest
import docker
from time import sleep
from datetime import datetime
from helpers.flatbuffer_helpers import check_expected_value
from helpers.producerwrapper import ProducerWrapper


CONFIG_TOPIC = "TEST_forwarderConfig"


# Skipped by default, Comment out to enable
@pytest.mark.skip(reason="Long running test disabled by default")
def test_long_run(docker_compose_lr):
    """
    Test that the channel defined in the config file is created.

    :param docker_compose: Test fixture
    :return: None
    """
    data_topic = "TEST_forwarderDataLR"
    sleep(5)
    producer = ProducerWrapper("localhost:9092", CONFIG_TOPIC, data_topic)
    producer.add_config([PVDOUBLE])

    # Set up consumer now and subscribe from earliest offset on data topic
    cons = create_consumer("earliest")
    cons.subscribe([data_topic])
    with open("logs/forwarder_lr_stats.log", "w+") as stats_file:
        with open("logs/forwarder_lr_missedupdates.log", "w+") as file:
            for i in range(5150):  # minimum 12 hours with 4 second sleep time
                # Change pv value now
                change_pv_value(PVDOUBLE, i)
                # Wait for the forwarder to push the update
                sleep(3)
                try:
                    msg, _ = poll_for_valid_message(cons)
                except MsgErrorException:
                    sleep(3)
                    msg, _ = poll_for_valid_message(cons)
                try:
                    check_expected_value(msg, PVDOUBLE, float(i))
                except AssertionError:
                    # Message is either incorrect or empty - log expected value to file
                    file.write(str(i) + "\n")
                container = False
                # Report stats every 10th iteration
                if i % 10 == 0:
                    client = docker.from_env()
                    for item in client.containers.list():
                        if "forwarder" in item.name:
                            container = item
                            break
                    if container:
                        stats_file.write(
                            "{}\t{}\n".format(
                                datetime.now(),
                                container.stats(stream=False)["memory_stats"]["usage"],
                            )
                        )
