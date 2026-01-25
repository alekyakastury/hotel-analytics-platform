#!/usr/bin/env bash
set -euo pipefail

# Make a writable project data dir
export DATA_DIR="${DATA_DIR:-/tmp/hotel_analytics}"
mkdir -p "$DATA_DIR"

# Start Airflow in single-container mode (webserver + scheduler)
exec airflow standalone
