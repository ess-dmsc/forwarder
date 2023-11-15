#!/bin/sh

echo "Stopping and destroying containers..."
docker compose down

echo "Clean-up completed!"
