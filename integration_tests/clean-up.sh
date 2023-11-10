#!/bin/sh

echo "Stopping and destroying containers..."
source test_env/bin/activate
docker compose down

echo "Removing directories..."
rm -rf test_env || true
rm -rf shared_source/* || true

echo "Clean-up completed!"
