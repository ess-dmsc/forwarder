import os.path
import pytest
from compose.cli.main import TopLevelCommand, project_from_options
from confluent_kafka.admin import AdminClient
from confluent_kafka import Producer
import docker
from time import sleep
from subprocess import Popen
import warnings
from streaming_data_types.forwarder_config_update_rf5k import (
    serialise_rf5k,
    StreamInfo,
    Protocol,
)
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)

from .helpers.PVs import PVSTR, PVLONG


LOCAL_BUILD = "--local-build"
WAIT_FOR_DEBUGGER_ATTACH = "--wait-to-attach-debugger"


def pytest_addoption(parser):
    parser.addoption(
        LOCAL_BUILD,
        action="store",
        default=None,
        help="Directory of local build to run tests against instead of rebuilding in a container",
    )
    parser.addoption(
        WAIT_FOR_DEBUGGER_ATTACH,
        type=bool,
        action="store",
        default=False,
        help=f"Use this flag when using the {LOCAL_BUILD} option to cause the system "
        f"tests to prompt you to attach a debugger to the file writer process",
    )


def wait_until_kafka_ready(docker_cmd, docker_options):
    print("Waiting for Kafka broker to be ready for system tests...", flush=True)
    conf = {
        "bootstrap.servers": "localhost:9092",
        "metadata.request.timeout.ms": "10000",
    }
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


@pytest.fixture(scope="session", autouse=True)
def build_forwarder_image(request):
    # Only build image if we are not running against a local build
    if request.config.getoption(LOCAL_BUILD) is None:
        client = docker.from_env()
        print("Building Forwarder image", flush=True)
        build_args = {}
        if "http_proxy" in os.environ:
            build_args["http_proxy"] = os.environ["http_proxy"]
        if "https_proxy" in os.environ:
            build_args["https_proxy"] = os.environ["https_proxy"]
        if "local_conan_server" in os.environ:
            build_args["local_conan_server"] = os.environ["local_conan_server"]
        image, logs = client.images.build(
            path="../", tag="forwarder:latest", rm=False, buildargs=build_args
        )
        for item in logs:
            print(item, flush=True)


def run_containers(cmd, options):
    print("Running docker-compose up", flush=True)
    cmd.up(options)
    print("\nFinished docker-compose up\n", flush=True)


def build_and_run(options, request, config_file=None, log_file=None):
    local_path = request.config.getoption(LOCAL_BUILD)
    wait_for_debugger = request.config.getoption(WAIT_FOR_DEBUGGER_ATTACH)
    if wait_for_debugger and local_path is None:
        warnings.warn(
            "Option specified to wait for debugger to attach, but this "
            "can only be used if a local build path is provided"
        )

    if local_path is None:
        project = project_from_options(os.path.dirname(__file__), options)
        cmd = TopLevelCommand(project)
        run_containers(cmd, options)
    else:
        # Launch local build of forwarder
        full_path_of_forwarder_exe = os.path.join(local_path, "forwarder_launch.py")
        command_options = [
            "python",
            full_path_of_forwarder_exe,
            "-c",
            os.path.join(local_path, "system_tests", "config-files", config_file),
            "--log-file",
            os.path.join(local_path, "system_tests", "logs", log_file),
        ]
        proc = Popen(command_options)
        if wait_for_debugger:
            input(
                f"\n"
                f"Attach a debugger to process id {proc.pid} now if you wish, then press enter to continue the tests: "
            )

    def fin():
        # Stop the containers then remove them and their volumes (--volumes option)
        print("containers stopping", flush=True)
        if local_path is None:
            log_options = dict(options)
            log_options["SERVICE"] = ["forwarder"]
            cmd.logs(log_options)
            options["--timeout"] = 30
            cmd.down(options)
        else:
            proc.kill()
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
    """
    :type request: _pytest.python.FixtureRequest
    """
    print("Started preparing test environment...", flush=True)

    # Options must be given as long form
    options = common_options
    options["--project-name"] = "forwarderNoCommand"
    options["--file"] = ["compose/docker-compose-forwarding.yml"]

    build_and_run(
        options, request, "forwarder_config_forwarding.ini", "forwarder_tests.log"
    )


@pytest.fixture(scope="module")
def docker_compose_idle_updates(request):
    """
    :type request: _pytest.python.FixtureRequest
    """
    print("Started preparing test environment...", flush=True)

    # Options must be given as long form
    options = common_options
    options["--project-name"] = "idle"
    options["--file"] = ["compose/docker-compose-idle-updates.yml"]

    build_and_run(
        options, request, "forwarder_config_idle_updates.ini", "forwarder_tests.log"
    )


@pytest.fixture(scope="module", autouse=False)
def docker_compose_lr(request):
    """
    :type request: _pytest.python.FixtureRequest
    """
    print("Started preparing test environment...", flush=True)

    # Options must be given as long form
    options = common_options
    options["--project-name"] = "lr"
    options["--file"] = ["compose/docker-compose-long-running.yml"]

    build_and_run(options, request, "forwarder_config_lr.ini", "forwarder_tests.log")


@pytest.fixture(scope="module")
def docker_compose_storage(request):
    """
    :type request: _pytest.python.FixtureRequest
    """
    print("Started preparing test environment...", flush=True)

    # Push old configuration into kafka
    conf = {
        "bootstrap.servers": "localhost:9092",
        "metadata.request.timeout.ms": "10000",
    }
    producer = Producer(**conf)

    stream_1 = StreamInfo(PVSTR, "f142", "some_topic_1", Protocol.Protocol.CA)
    stream_2 = StreamInfo(PVLONG, "tdct", "some_topic_2", Protocol.Protocol.PVA)
    message = serialise_rf5k(UpdateType.ADD, [stream_1, stream_2])

    producer.produce("TEST_forwarderStorage", message)
    producer.flush()

    # Options must be given as long form
    options = common_options
    options["--project-name"] = "forwarderStorage"
    options["--file"] = ["compose/docker-compose-storage.yml"]

    build_and_run(
        options, request, "forwarder_config_storage.ini", "forwarder_tests.log"
    )
