#!/usr/bin/env bash
set -euo pipefail

# Bootstrap Airflow shared config (Variables + Connections)
# Applies inside the running docker-compose stack.
#
# Usage:
#   cd /home/envy/Lessons/airflow
#   sudo docker compose up -d
#   ./scripts/bootstrap_airflow.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Helper: run airflow CLI in standalone container (has airflow installed and DB access)
airflow_cli() {
  sudo docker compose exec -T airflow-standalone airflow "$@"
}

# Helper: ensure connection exists (create if missing)
ensure_conn() {
  local conn_id="$1"
  shift

  if airflow_cli connections get "$conn_id" >/dev/null 2>&1; then
    echo "[OK] connection already exists: ${conn_id}"
  else
    echo "[ADD] connection: ${conn_id}"
    airflow_cli connections add "$conn_id" "$@"
  fi
}

# Helper: ensure variable exists (set if missing)
ensure_var() {
  local key="$1"
  local value="$2"

  if airflow_cli variables get "$key" >/dev/null 2>&1; then
    echo "[OK] variable already exists: ${key}"
  else
    echo "[ADD] variable: ${key}"
    airflow_cli variables set "$key" "$value"
  fi
}

echo "== Airflow bootstrap (shared across DAGs) =="

# Connections used across DAGs in this repo:
# - minio (used by multiple DAGs via S3Hook)
# - fs_default (used by FileSensor)
# - postgres ("airflow" DB connection for Postgres hooks/operators)
# - postgres_data (separate Postgres used as target in lessons, port 5433)

# MinIO is already injected via env AIRFLOW_CONN_MINIO in docker-compose,
# but adding it as a real Connection makes UI and DAG portability nicer.
ensure_conn "minio" \
  --conn-type "aws" \
  --conn-extra '{"aws_access_key_id":"minioadmin","aws_secret_access_key":"minioadmin","host":"http://minio:9000"}'

# FileSystem connection for FileSensor
# This is a core sensor, not a provider: airflow.sensors.filesystem.FileSensor
ensure_conn "fs_default" \
  --conn-type "fs" \
  --conn-extra '{"path":"/"}'

# Airflow metadata DB (example usage by Postgres operators)
ensure_conn "postgres" \
  --conn-type "postgres" \
  --conn-host "postgres" \
  --conn-schema "airflow" \
  --conn-login "airflow" \
  --conn-password "airflow" \
  --conn-port "5432"

# Separate Postgres instance intended for lesson data loads (creds per your requirement)
ensure_conn "postgres_data" \
  --conn-type "postgres" \
  --conn-host "postgres_data" \
  --conn-schema "postgres" \
  --conn-login "postgres" \
  --conn-password "postgres" \
  --conn-port "5432"

# HTTP connection used by HttpSensor/HttpHook in multiple DAGs
# Uses base host; endpoints in DAGs can still pass full URL, но лучше опираться на host.
ensure_conn "http_default" \
  --conn-type "http" \
  --conn-host "https://catfact.ninja"

# TheCatAPI (HTTP)
# host должен быть БЕЗ схемы, а schema задаём отдельно.
ensure_conn "thecatapi_default" \
  --conn-type "http" \
  --conn-host "api.thecatapi.com" \
  --conn-schema "https" \
  --conn-extra '{}'

# Variables (shared paths/buckets used by multiple DAGs)
ensure_var "DATA_IMG_DIR" "/opt/airflow/data/img"
ensure_var "MINIO_BUCKET_CATS" "cats"
ensure_var "MINIO_RAW_PREFIX" "raw"
ensure_var "MINIO_IMAGES_PREFIX" "images"
ensure_var "MINIO_CONN_ID" "minio"
ensure_var "POSTGRES_CONN_ID" "postgres_data"
ensure_var "SALES_BUCKET" "sales-reports"
ensure_var "SALES_RAW_PREFIX" "raw_data"
ensure_var "SALES_REPORTS_PREFIX" "reports"
ensure_var "THECATAPI_CONN_ID" "thecatapi_default"

echo "== Done =="
