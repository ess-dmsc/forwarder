#!/bin/sh

set -e

echo "Python version:"
python --version

echo "Creating virtual environment..."
python -m venv test_env
source test_env/bin/activate

echo "Installing requirements..."
pip install --upgrade pip
pip install docker-compose 'requests<2.30.0'

echo "Preparing test..."
mkdir -p shared_volume
rm -rf shared_volume/*

echo "Preparation completed!"
