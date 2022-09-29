import os.path
from subprocess import Popen
from time import sleep

import pytest
from compose.cli.main import TopLevelCommand, project_from_options
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)
from streaming_data_types.forwarder_config_update_rf5k import (
    Protocol,
    StreamInfo,
    serialise_rf5k,
)

from forwarder.kafka.kafka_helpers import get_sasl_config

from .helpers.PVs import PVLONG, PVSTR

WAIT_FOR_DEBUGGER_ATTACH = "--wait-to-attach-debugger"


def pytest_addoption(parser):
    parser.addoption(
        WAIT_FOR_DEBUGGER_ATTACH,
        type=bool,
        action="store",
        default=False,
        help="Use this flag to cause the integration tests to prompt you to attach a debugger to the file writer process",
    )


def wait_until_kafka_ready(docker_cmd, docker_options):
    print("Waiting for Kafka broker to be ready for integration tests...", flush=True)
    conf = {
        "bootstrap.servers": "localhost:9092",
    }
    conf.update(get_sasl_config("PLAIN", "client", "client-secret"))
    producer = Producer(**conf)

    kafka_ready = False

    def delivery_callback(err, msg):
        nonlocal n_polls
        nonlocal kafka_ready
        if not err:
            print("Kafka is ready!")
            kafka_ready = True

    n_polls = 0
    while n_polls < 10 and not kafka_ready:
        producer.produce(
            "waitUntilUp", value="Test message", on_delivery=delivery_callback
        )
        producer.poll(10)
        n_polls += 1

    if not kafka_ready:
        docker_cmd.down(docker_options)  # Bring down containers cleanly
        raise Exception("Kafka broker was not ready after 100 seconds, aborting tests.")

    client = AdminClient(conf)
    topic_list = [
        NewTopic("TEST_forwarderConfig", 1, 1),
        NewTopic("TEST_forwarderData_2_partitions", 2, 1),
        NewTopic("TEST_forwarderData_0", 1, 1),
        NewTopic("TEST_forwarderData_1", 1, 1),
        NewTopic("TEST_forwarderData_2", 1, 1),
        NewTopic("TEST_forwarderData_change_config", 1, 1),
        NewTopic("TEST_forwarderData_connection_status", 1, 1),
        NewTopic("TEST_forwarderData_fake", 1, 1),
        NewTopic("TEST_forwarderData_idle_updates", 1, 1),
        NewTopic("TEST_forwarderStorage", 1, 1),
        NewTopic("TEST_forwarderStorageStatus", 1, 1),
    ]
    client.create_topics(topic_list)
    topic_ready = False

    n_polls = 0
    while n_polls < 10 and not topic_ready:
        if "TEST_forwarderConfig" in client.list_topics().topics.keys():
            topic_ready = True
            print("Topic is ready!", flush=True)
            break
        sleep(6)
        n_polls += 1

    if not topic_ready:
        docker_cmd.down(docker_options)  # Bring down containers cleanly
        raise Exception("Kafka topic was not ready after 60 seconds, aborting tests.")


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


def run_containers(cmd, options):
    print("Running docker-compose up", flush=True)
    cmd.up(options)
    print("\nFinished docker-compose up\n", flush=True)


def build_and_run(request, config_file=None, log_file=None):
    wait_for_debugger = request.config.getoption(WAIT_FOR_DEBUGGER_ATTACH)

    local_path = "../"
    # Launch local builds
    proc_ca_ioc = Popen(
        ["python", os.path.join(local_path, "integration_tests/helpers/ca_ioc.py")]
    )
    proc_pva_ioc = Popen(
        ["python", os.path.join(local_path, "integration_tests/helpers/pva_ioc.py")]
    )

    forwarder_path = os.path.join(local_path, "forwarder_launch.py")
    command_options = [
        "python",
        forwarder_path,
        "-c",
        os.path.join(local_path, "integration_tests", "config-files", config_file),
        "--log-file",
        os.path.join(local_path, "integration_tests", "logs", log_file),
    ]
    proc_forwarder = Popen(command_options)
    if wait_for_debugger:
        input(
            f"\n"
            f"Attach a debugger to process id {proc_forwarder.pid} now if you wish, then press enter to continue the tests: "
        )

    def fin():
        # Stop the containers then remove them and their volumes (--volumes option)
        print("containers stopping", flush=True)
        proc_forwarder.kill()
        proc_ca_ioc.kill()
        proc_pva_ioc.kill()
        print("containers stopped", flush=True)

    # Using a finalizer rather than yield in the fixture means
    # that the containers will be brought down even if tests fail
    request.addfinalizer(fin)


@pytest.fixture(scope="session", autouse=True)
def remove_logs_from_previous_run(request):
    print("Removing previous log files", flush=True)
    dir_name = os.path.join(os.getcwd(), "logs")
    dirlist = os.listdir(dir_name)
    for filename in dirlist:
        if filename.endswith(".log"):
            os.remove(os.path.join(dir_name, filename))
    print("Removed previous log files", flush=True)


@pytest.fixture(scope="session", autouse=True)
def start_kafka(request):
    print("Starting zookeeper and kafka", flush=True)
    options = common_options
    options["--project-name"] = "kafka"
    options["--file"] = ["compose/docker-compose-kafka.yml"]
    project = project_from_options(os.path.dirname(__file__), options)
    cmd = TopLevelCommand(project)

    cmd.up(options)
    print("Started kafka containers", flush=True)
    wait_until_kafka_ready(cmd, options)

    def fin():
        print("Stopping zookeeper and kafka", flush=True)
        options["--timeout"] = 30
        options["--project-name"] = "kafka"
        options["--file"] = ["compose/docker-compose-kafka.yml"]
        cmd.down(options)

    request.addfinalizer(fin)


@pytest.fixture(scope="module")
def docker_compose_forwarding(request):
    build_and_run(request, "forwarder_config_forwarding.ini", "forwarder_tests.log")


@pytest.fixture(scope="module")
def docker_compose_idle_updates(request):
    build_and_run(request, "forwarder_config_idle_updates.ini", "forwarder_tests.log")


@pytest.fixture(scope="module", autouse=False)
def docker_compose_lr(request):
    build_and_run(request, "forwarder_config_lr.ini", "forwarder_tests.log")


@pytest.fixture(scope="module")
def docker_compose_storage(request):
    """
    :type request: _pytest.python.FixtureRequest
    """
    print("Started preparing test environment...", flush=True)

    # Push old configuration into kafka
    conf = {
        "bootstrap.servers": "localhost:9092",
    }
    conf.update(get_sasl_config("PLAIN", "client", "client-secret"))
    producer = Producer(**conf)

    stream_1 = StreamInfo(PVSTR, "f142", "some_topic_1", Protocol.Protocol.CA)
    stream_2 = StreamInfo(PVLONG, "tdct", "some_topic_2", Protocol.Protocol.PVA)
    message = serialise_rf5k(UpdateType.ADD, [stream_1, stream_2])

    producer.produce("TEST_forwarderStorage", message)
    producer.flush()

    build_and_run(request, "forwarder_config_storage.ini", "forwarder_tests.log")
