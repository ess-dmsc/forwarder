## System tests

The system tests consist of a series of automated tests for this repository that test it in a way similar to how it would be used in production.

It uses Docker containers to create containerised instances of Kafka and other components.

### Usage

[optional] Set up a Python 3.6+ virtual environment and activate it (see [here](https://virtualenv.pypa.io/en/stable/))

* Install Docker

* Install the requirements using pip: `pip install -r system-tests/requirements.txt`

* Stop and remove any containers that may interfere with the system tests, e.g IOC containers, Kafka containers and 
containers from previous runs. To stop and remove all containers use `docker stop $(docker ps -a -q) && docker 
rm $(docker ps -a -q)`

* Run `pytest -s .` from the `system-tests/` directory

* Wait

Note: these tests take some time to run.

Append `--local-build <PATH_TO_BUILD_DIRECTORY>` to the `pytest` command to run the system tests against a local build 
of the Forwarder instead of rebuilding in a container.

Append `--wait-to-attach-debugger` to the `pytest` command to pause running the system tests until a debugger has 
been attached. This option only works in conjunction with the `--local-build` option. A message will appear in the 
console and it will report the process ID and wait for user input before continuing. However note that the Forwarder 
process itself continues running so you may need to find the debugger message amoung other output, I haven't found a 
solution to this as SIGSTOP seems to causes the Forwarder process to terminate rather than pause.

### General Architecture

The system tests use pytest for the test runner, and use separate fixtures for different configurations of the forwarder. 

Firstly, the system tests attempt to build and tag the latest forwarder image. This can take a few minutes if it needs to download the base docker images.

The IOC, Kafka and Zookeeper containers are started with `docker-compose` and persist throughout all of the tests, and when finished will be stopped and removed. 

Each fixture starts the forwarder with an `ini` config file (found in `/config-files`), and in some cases use a JSON command at startup so nothing has to be sent. 

In some tests, command messages in `JSON` form are sent to kafka to change the configuration of the forwarder during testing. 

Most tests poll from Kafka to check against PV values and in some cases consume everything from the status topic.

Log files are placed in the `logs` folder in `system-tests` provided that the `ini` file is using `--log-file` and the docker-compose file mounts the `logs` directory.

### Developer notes

There are helper functions for the Kafka interface of the system tests as well as FlatBuffers and EPICS helpers. These are found in the `helpers` folder. 

To create a new fixture, a new function should be added in `conftest.py` as well as a docker compose file in `compose/` and a startup `ini` config file. The test itself should be created in a file with the prefix `test_`, for example `test_idle_pv_updates`, so that file can be picked up by pytest. 

The fixture name must be used as the first parameter to the test like so: 
`def test_forwarder_sends_idle_pv_updates(docker_compose_idle_updates):`

### Creating tests

To create a new fixture, a new function should be added in `conftest.py` as well as a docker compose file in `compose/` and a startup `ini` config file. The test itself should be created in a file with the prefix `test_`, for example `test_idle_pv_updates`, so that file can be picked up by pytest. 

The fixture name must be used as the first parameter to the test like so:
`def test_data_reaches_file(docker_compose):`
