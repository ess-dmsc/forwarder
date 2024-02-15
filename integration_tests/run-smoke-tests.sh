#!/bin/sh

echo "Running smoke tests..."

set -e

docker exec ${FORWARDER_FORWARDER_CONTAINER_NAME:-forwarder} bash -c 'cd forwarder/integration_tests/smoke_tests; scl enable rh-python38 -- python prepare.py'
docker exec ${FORWARDER_FORWARDER_CONTAINER_NAME:-forwarder} bash -c 'cd forwarder; scl enable rh-python38 -- python forwarder_launch.py --config-topic=kafka1:9092/forwarder_commands --status-topic=kafka1:9092/forwarder_status --storage-topic=kafka1:9092/forwarder_storage --output-broker=kafka1:9092 --pv-update-period=10000' &

echo "Sleeping..."
sleep 30
echo "Continuing!"

set +e

docker exec ${FORWARDER_FORWARDER_CONTAINER_NAME:-forwarder} bash -c 'cd forwarder/integration_tests/smoke_tests; scl enable rh-python38 -- ~/.local/bin/pytest --junitxml=SmokeTestsOutput.xml'

result=$?

docker cp ${FORWARDER_FORWARDER_CONTAINER_NAME:-forwarder}:/home/jenkins/forwarder/integration_tests/smoke_tests/SmokeTestsOutput.xml .

exit $result
