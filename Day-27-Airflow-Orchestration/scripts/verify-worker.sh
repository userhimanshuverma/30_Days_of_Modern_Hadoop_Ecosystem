#!/usr/bin/env bash
# scripts/verify-worker.sh
# Audits worker health status and queue latency using the Celery command-line utility

set -eo pipefail

echo "========================================================="
echo "🐝 CELERY WORKERS DIAGNOSTIC AUDIT"
echo "========================================================="

# Verify Docker Compose is running worker node
if ! docker compose ps | grep -q "worker"; then
    echo "🔴 ERROR: Celery Worker container is not running."
    echo "This script requires CeleryExecutor stack to be active."
    exit 1
fi

# 1. Ping Celery Worker pool
echo "🔍 Pinging Celery workers via Celery control API..."
PING_STATUS=$(docker compose exec -T worker celery -A airflow.providers.celery.executors.celery_executor control ping 2>&1 || echo "failed")

if echo "$PING_STATUS" | grep -q "OK"; then
    echo "🟢 Workers ping status: OK"
    echo "$PING_STATUS"
else
    echo "🔴 ERROR: Workers are unreachable or not responding."
    echo "$PING_STATUS"
fi

# 2. Query worker capacities and stats
echo "🔍 Extracting worker concurrency and queue configurations..."
docker compose exec -T worker celery -A airflow.providers.celery.executors.celery_executor stats | grep -E "concurrency|pool|broker" || echo "🟡 No active stats returned."

echo "========================================================="
