#!/bin/sh
set -e

: "${PREFECT_API_URL:?PREFECT_API_URL must be set}"
: "${WORK_POOL_NAME:=default-pool}"

echo "⏳ Waiting for Prefect server at ${PREFECT_API_URL}..."
until python -c "import urllib.request, sys; urllib.request.urlopen('${PREFECT_API_URL}/health', timeout=2)" 2>/dev/null; do
    sleep 2
done
echo "✅ Prefect server reachable"

echo "🛠  Ensuring work pool '${WORK_POOL_NAME}' exists..."
prefect work-pool create --type process "${WORK_POOL_NAME}" --overwrite

echo "🚀 Deploying flows from prefect.yaml..."
prefect deploy --all --pool "${WORK_POOL_NAME}"

echo "👷 Starting worker on pool '${WORK_POOL_NAME}'..."
exec prefect worker start --pool "${WORK_POOL_NAME}"
