#!/bin/sh
set -euo pipefail

until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; do
  echo "Waiting for MinIO to accept connections..."
  sleep 2
done
mc mb -p local/airflow-logs || true
mc mb -p local/ge-artifacts || true
mc anonymous set download local/airflow-logs || true
mc anonymous set download local/ge-artifacts || true
