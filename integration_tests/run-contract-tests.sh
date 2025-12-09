#!/bin/sh

echo "Running contract tests..."

docker exec ${FORWARDER_FORWARDER_CONTAINER_NAME:-forwarder} bash -c 'cd forwarder/integration_tests/contract_tests; uv run pytest --junitxml=ContractTestsOutput.xml'

result=$?

docker cp ${FORWARDER_FORWARDER_CONTAINER_NAME:-forwarder}:/home/jenkins/forwarder/integration_tests/contract_tests/ContractTestsOutput.xml .

exit $result
