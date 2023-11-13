#!/bin/sh

set -e

echo "Running smoke tests..."

docker exec integration_tests-forwarder-1 bash -c 'cd /shared_source/forwarder/integration_tests/smoke_tests; scl enable rh-python38 -- python prepare.py'
docker exec integration_tests-forwarder-1 bash -c 'cd /shared_source/forwarder/; scl enable rh-python38 -- python forwarder_launch.py --config-topic=kafka1:9092/forwarder_commands --status-topic=kafka1:9092/forwarder_status --storage-topic=kafka1:9092/forwarder_storage --output-broker=kafka1:9092 --pv-update-period=10000' &

echo "Sleeping..."
sleep 30
echo "Continuing!"

docker exec integration_tests-forwarder-1 bash -c 'cd /shared_source/forwarder/integration_tests/smoke_tests; scl enable rh-python38 -- ~/.local/bin/pytest --junitxml=SmokeTestsOutput.xml'

cp shared_volume/forwarder/integration_tests/smoke_tests/SmokeTestsOutput.xml .
