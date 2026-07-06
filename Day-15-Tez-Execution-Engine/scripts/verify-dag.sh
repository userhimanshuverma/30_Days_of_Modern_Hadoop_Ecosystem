#!/bin/bash
# verify-dag.sh - Inspects YARN Resource Manager to check Tez DAG jobs and their status.

set -e

RM_HOST=${1:-"resourcemanager"}
RM_PORT=${2:-"8088"}

echo "=== 🔍 STEP 1: Querying YARN Applications from YARN Resource Manager ($RM_HOST:$RM_PORT) ==="
APP_LIST=$(curl -s "http://$RM_HOST:$RM_PORT/ws/v1/cluster/apps")

if [ -z "$APP_LIST" ]; then
    echo "❌ Error: YARN Resource Manager API did not return data. Ensure YARN is running."
    exit 1
fi

echo "✅ Success: Received app response from YARN."

# Parse YARN applications using simple grep/sed or print summaries
TEZ_SESSIONS=$(echo "$APP_LIST" | grep -o '"applicationType":"[^"]*"' | wc -l || echo 0)
echo "Total Applications registered in YARN: $TEZ_SESSIONS"

# Check if there are any active Tez sessions or jobs
if echo "$APP_LIST" | grep -q "TEZ"; then
    echo "✅ Success: Found application(s) with TEZ type."
    echo "Details:"
    echo "$APP_LIST" | grep -o -E '"id":"application_[0-9]+_[0-9]+","user":"[a-zA-Z0-9]+","name":"[^"]+","queue":"[^"]+","state":"[A-Z]+","finalStatus":"[A-Z]+"' | sed 's/"//g'
else
    echo "ℹ️ Note: No active or completed Tez applications are currently listed in YARN."
    echo "Submit a Hive query or run the demo script to populate DAG executions in YARN."
fi

echo "=== 🎉 DAG Verification Complete ==="
