#!/bin/sh

set -e

echo "Running contract tests..."

docker exec integration_tests-forwarder-1 bash -c 'cd /shared_source/forwarder/integration_tests/contract_tests; scl enable rh-python38 -- ~/.local/bin/pytest --junitxml=ContractTestsOutput.xml'

cp shared_volume/forwarder/integration_tests/contract_tests/ContractTestsOutput.xml .
