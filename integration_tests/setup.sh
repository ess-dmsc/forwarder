#!/bin/sh

set -e

echo "Preparing test..."
docker compose up -d

echo "Sleeping..."
sleep 10
echo "Continuing!"

docker cp .. ${FORWARDER_FORWARDER_CONTAINER_NAME:-forwarder}:/home/jenkins/

docker exec ${FORWARDER_FORWARDER_CONTAINER_NAME:-forwarder} bash -c "scl enable rh-python38 -- python -m pip install --user --proxy='$HTTPS_PROXY' -r forwarder/requirements.txt"
docker exec ${FORWARDER_FORWARDER_CONTAINER_NAME:-forwarder} bash -c "scl enable rh-python38 -- python -m pip install --user --proxy='$HTTPS_PROXY' pytest"

echo "Preparation completed!"
