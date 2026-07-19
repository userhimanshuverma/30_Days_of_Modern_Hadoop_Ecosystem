#!/usr/bin/env bash
# scripts/verify-etl.sh
# Triggers the ETL pipeline via the Airflow CLI and monitors its execution stages

set -eo pipefail

DAG_ID="day_27_hands_on_etl"

echo "========================================================="
echo "🚀 PIPELINE EXECUTION VERIFICATION SYSTEM"
echo "========================================================="

# Verify Docker Compose is running
if ! docker compose ps | grep -q "scheduler"; then
    echo "🔴 ERROR: Airflow services are offline."
    exit 1
fi

# 1. Unpause the target DAG
echo "🔍 Unpausing DAG: $DAG_ID ..."
docker compose exec -T scheduler airflow dags unpause "$DAG_ID"

# 2. Trigger the DAG manually
echo "🔍 Triggering dynamic run..."
RUN_OUTPUT=$(docker compose exec -T scheduler airflow dags trigger "$DAG_ID")
echo "Output: $RUN_OUTPUT"
RUN_ID=$(echo "$RUN_OUTPUT" | grep -o -E "manual__[a-zA-Z0-9_.:+-]+" | head -n 1)
echo "🟢 Run initiated successfully with Run ID: $RUN_ID"

# 3. Monitor execution loop state
echo "🔍 Tracking task transitions (timeout in 60s)..."
for i in {1..12}; do
    sleep 5
    STATE=$(docker compose exec -T scheduler airflow dags state -r "$RUN_ID" "$DAG_ID" 2>/dev/null || echo "running")
    # Clean output whitespace
    STATE=$(echo "$STATE" | tr -d '[:space:]')
    echo "[$((i*5))s] Current DAG State: $STATE"
    if [ "$STATE" == "success" ]; then
        echo "🟢 PIPELINE SUCCESS: All tasks completed within target SLA!"
        break
    elif [ "$STATE" == "failed" ]; then
        echo "🔴 PIPELINE FAILURE: Task execution returned critical errors."
        echo "Check task instance failures using:"
        echo "  docker compose exec scheduler airflow tasks states-for-dag-run -r $RUN_ID $DAG_ID"
        exit 1
    fi
done

# 4. Print results from tasks list status
echo "🔍 Summary of task instance outcomes for this run:"
docker compose exec -T scheduler airflow tasks states-for-dag-run -r "$RUN_ID" "$DAG_ID" || true

echo "========================================================="
