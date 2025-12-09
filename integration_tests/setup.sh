#!/bin/sh

set -e

echo "Preparing test..."
docker compose up -d

echo "Sleeping..."
sleep 10
echo "Continuing!"

docker cp .. ${FORWARDER_FORWARDER_CONTAINER_NAME:-forwarder}:/home/jenkins/

echo "Preparation completed!"
