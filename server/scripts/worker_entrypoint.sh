#!/bin/bash

echo "Checking Prefect GCP dependency installation"
pip install "prefect-gcp[cloud_run_v2]==0.6.17" --quiet --no-input

echo "Starting 'gcp-wp' Work Pool"
prefect worker start --pool "gcp-wp"
