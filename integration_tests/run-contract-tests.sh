#!/bin/sh

echo "Running contract tests..."

docker exec ${FRDR_FORWARDER_CONTAINER_NAME:-forwarder} bash -c 'cd forwarder/integration_tests/contract_tests; scl enable rh-python38 -- ~/.local/bin/pytest --junitxml=ContractTestsOutput.xml'

result=$?

docker cp ${FRDR_FORWARDER_CONTAINER_NAME:-forwarder}:/home/jenkins/forwarder/integration_tests/contract_tests/ContractTestsOutput.xml .

exit $result
