#!/bin/sh

echo "Stopping and destroying containers..."
docker compose down

echo "Removing directories..."
rm -rf shared_source/* || true

echo "Clean-up completed!"
