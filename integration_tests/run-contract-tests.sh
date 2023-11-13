#!/bin/sh

set -e

echo "Running contract tests..."

docker exec integration_tests-forwarder-1 bash -c 'cd forwarder/integration_tests/contract_tests; scl enable rh-python38 -- ~/.local/bin/pytest --junitxml=ContractTestsOutput.xml'

docker cp integration_tests-forwarder-1:/home/jenkins/forwarder/integration_tests/contract_tests/ContractTestsOutput.xml .
