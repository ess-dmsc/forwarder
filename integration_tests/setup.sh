#!/bin/sh

set -e

echo "Preparing test..."
mkdir -p shared_volume
rm -rf shared_volume/*

docker compose up -d

echo "Sleeping..."
sleep 10
echo "Continuing!"

rsync -av .. shared_volume/forwarder --exclude=integration_test/shared_volume --exclude=".*" --exclude=venv

docker exec integration_tests-forwarder-1 bash -c 'cd /shared_source/forwarder/; scl enable rh-python38 -- python -m pip install --user --proxy "$HTTPS_PROXY" -r requirements.txt'
docker exec integration_tests-forwarder-1 bash -c 'cd /shared_source/forwarder/; scl enable rh-python38 -- python -m pip install --user --proxy "$HTTPS_PROXY" pytest'

echo "Preparation completed!"
