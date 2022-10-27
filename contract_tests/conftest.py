import os.path
import time
from time import sleep

import pytest
from compose.cli.main import TopLevelCommand, project_from_options
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

common_options = {
    "--no-deps": False,
    "--always-recreate-deps": False,
    "--scale": "",
    "--abort-on-container-exit": False,
    "SERVICE": "",
    "--remove-orphans": False,
    "--no-recreate": True,
    "--force-recreate": False,
    "--no-build": False,
    "--no-color": False,
    "--rmi": "none",
    "--volumes": True,  # Remove volumes when docker-compose down (don't persist kafka and zk data)
    "--follow": False,
    "--timestamps": False,
    "--tail": "all",
    "--detach": True,
    "--build": False,
    "--no-log-prefix": False,
}


def wait_until_kafka_ready(docker_cmd, docker_options):
    print("Waiting for Kafka broker to be ready for integration tests...")
    conf = {"bootstrap.servers": "localhost:9092", "api.version.request": True}
    producer = Producer(**conf)
    kafka_ready = False

    def delivery_callback(err, msg):
        nonlocal kafka_ready
        if not err and not kafka_ready:
            print("Kafka is ready!")
            kafka_ready = True

    wait_for = 60
    start_time = time.monotonic()
    while not kafka_ready:
        if time.monotonic() > start_time + wait_for:
            break
        producer.produce(
            "waitUntilUp", value="Test message", on_delivery=delivery_callback
        )
        producer.poll(1)

    if not kafka_ready:
        docker_cmd.down(docker_options)  # Bring down containers cleanly
        raise Exception(
            f"Kafka broker was not ready after {wait_for} seconds, aborting tests."
        )

    client = AdminClient(conf)
    topics_ready = False

    start_time = time.monotonic()
    while not topics_ready:
        if time.monotonic() > start_time + wait_for:
            break
        topics = client.list_topics().topics.keys()
        topics_needed = ["test_write_and_read_message"]
        present = [t in topics for t in topics_needed]
        if all(present):
            topics_ready = True
            print("Topics are ready!", flush=True)
            break
        sleep(5)

    if not topics_ready:
        docker_cmd.down(docker_options)  # Bring down containers cleanly
        raise Exception(
            f"Kafka topics were not ready after {wait_for} seconds, aborting tests."
        )


@pytest.fixture(scope="session", autouse=True)
def start_kafka(request):
    print("Starting zookeeper and kafka", flush=True)
    options = common_options
    options["--project-name"] = "kafka"
    options["--file"] = ["docker-compose.yml"]
    project = project_from_options(os.path.dirname(__file__), options)
    cmd = TopLevelCommand(project)

    cmd.up(options)
    print("Started kafka containers", flush=True)
    wait_until_kafka_ready(cmd, options)

    def fin():
        print("Stopping zookeeper and kafka", flush=True)
        options["--timeout"] = 30
        options["--project-name"] = "kafka"
        options["--file"] = ["docker-compose.yml"]
        cmd.down(options)

    request.addfinalizer(fin)
