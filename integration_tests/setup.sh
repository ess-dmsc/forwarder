#!/bin/sh

set -e

echo "Preparing test..."
docker compose up -d

echo "Sleeping..."
sleep 10
echo "Continuing!"

docker cp .. integration_tests-forwarder-1:/home/jenkins/

docker exec integration_tests-forwarder-1 bash -c 'scl enable rh-python38 -- python -m pip install --user -r forwarder/requirements.txt'
docker exec integration_tests-forwarder-1 bash -c 'scl enable rh-python38 -- python -m pip install --user pytest'

echo "Preparation completed!"
