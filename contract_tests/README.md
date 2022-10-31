## Contract tests

These tests test the contract/API of the key third-party services that are used by this application. E.g. Kafka and EPICS.

If these tests fail when a library is updated then it means that a library has changed their API and our code needs to be 
changed to handle this.

### Usage

* Install Docker and Docker Compose

* Run the contract tests
```commandline
cd contract_tests
docker compose up
# Give docker a few seconds to start up
# From another terminal
docker exec contract_tests_bash_1 bash -c 'cd forwarder/contract_tests; pytest .'
```

### What is going on?

Docker Compose starts up images for the services plus one for running the tests (contract_tests_bash_1). EPICS in a Docker 
image doesn't play well with the standard EPICS tools (pvget, etc.) running on the host machine (especially on MacOS).
To get round this we create this testing image which is used to run the tests.

This image has a copy of the repo on it, so logging onto it means one can play around with it. The command for this is:
`docker exec -it contract_tests-bash-1 bash `