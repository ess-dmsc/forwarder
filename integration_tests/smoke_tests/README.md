## Smoke tests

These tests test that the software basically works as expected with all the components running.

If these tests fail then it is likely that a change to the "system" has broken something.

https://en.wikipedia.org/wiki/Smoke_testing_(software)

### Usage

* Install Docker and Docker Compose

* Run the smoke tests
```commandline
cd integration_tests
docker compose up
# Give docker a few seconds to start up
# From another terminal, clone the repo, checkout the correct branch and then run the forwarder:
docker exec integration_tests_forwarder_1 bash -c 'git clone https://github.com/ess-dmsc/forwarder.git'
docker exec integration_tests_forwarder_1 bash -c 'cd forwarder/; git fetch; git checkout <BRANCH_NAME>; git pull'
docker exec integration_tests_forwarder_1 bash -c 'cd forwarder/; python -m pip install -r requirements.txt'
docker exec integration_tests_forwarder_1 bash -c 'cd forwarder/; python -m pip install pytest'
docker exec integration_tests_forwarder_1 bash -c 'cd shared_source/forwarder/integration_tests/smoke_tests; python create_topics.py'
docker exec integration_tests_forwarder_1 bash -c 'cd shared_source/forwarder/; python forwarder_launch.py --config-topic=kafka:9092/forwarder_commands --status-topic=kafka:9092/forwarder_status --storage-topic=kafka:9092/forwarder_storage --output-broker=kafka:9092 --pv-update-period=10000'
# From another terminal, run the smoke tests:
docker exec integration_tests_forwarder_1 bash -c 'cd shared_source/forwarder/integration_tests/smoke_tests; pytest'       
```
Alternatively, one can rsync the local copy of the repo on to the docker image rather than clone it:
```
# On the local machine from the repo's top directory
rsync -av . shared_volume/forwarder --exclude=shared_volume --exclude=".*" 
```
Then:
```
docker exec integration_tests_forwarder_1 bash -c 'cd shared_source/forwarder/; python -m pip install -r requirements.txt'
docker exec integration_tests_forwarder_1 bash -c 'cd /shared_source/forwarder/integration_tests/contract_tests; pytest .'
```

### What is going on?

Docker Compose starts up images for the services (Kafka, etc.) plus one for our use (called forwarder). This machine can be
used for running tests, etc.

EPICS in a Docker image doesn't play well with the standard EPICS tools (pvget, etc.) running on the host machine 
(especially on MacOS).
To get round this we create this testing image which is used to run the tests.

This image has a copy of the repo on it, so logging onto it means one can play around with it. The command for this is:
`docker exec -it integration_tests_forwarder_1 bash`