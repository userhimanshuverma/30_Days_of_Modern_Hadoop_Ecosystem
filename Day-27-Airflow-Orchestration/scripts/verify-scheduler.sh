#!/usr/bin/env bash
# scripts/verify-scheduler.sh
# Checks scheduler daemon process states, active loops, and scheduler job records

set -eo pipefail

echo "========================================================="
echo "⚙️ SCHEDULER HEALTH AND LATENCY REPORT"
echo "========================================================="

# Verify Docker Compose is running
if ! docker compose ps | grep -q "scheduler"; then
    echo "🔴 ERROR: Airflow Scheduler container is not running."
    echo "Start it via: docker compose up -d scheduler"
    exit 1
fi

# 1. Print Airflow CLI Scheduler status
echo "🔍 Extracting Scheduler Job parameters..."
docker compose exec -T scheduler airflow jobs check --job-type SchedulerJob
echo "🟢 Scheduler process is alive and updating metadata database heartbeat."

# 2. Check parsing speeds and latency of files
echo "🔍 Querying parsing logs inside scheduler container..."
PARSE_SUMMARY=$(docker compose exec -T scheduler tail -n 50 /opt/airflow/logs/dag_processor_manager.log 2>/dev/null || echo "Log file not found (normal if standalone processor disabled)")
if [ -n "$PARSE_SUMMARY" ]; then
    echo "$PARSE_SUMMARY" | grep -E "DAGs|processed" || echo "🟡 No parsing cycle reports in direct logs yet."
fi

# 3. Print loaded DAG definitions
echo "🔍 Querying registered DAGs in DB store:"
docker compose exec -T scheduler airflow dags list

echo "========================================================="
