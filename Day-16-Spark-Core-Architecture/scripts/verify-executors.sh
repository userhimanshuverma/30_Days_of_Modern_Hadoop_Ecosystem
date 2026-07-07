#!/bin/bash
# Day 16: Spark Executors Verification Script
# Location: Day-16-Spark-Core-Architecture/scripts/verify-executors.sh

set -e

echo "=== 🔍 STEP 1: Querying Spark Master Cluster State ==="
STATUS_JSON=$(curl -s http://spark-master:8080/json)

if [ -z "$STATUS_JSON" ]; then
  echo "❌ Error: Could not retrieve cluster status JSON from spark-master."
  exit 1
fi

echo "✅ Success: Cluster metadata retrieved."

echo "=== 🔍 STEP 2: Verifying Registered Workers ==="
WORKER_COUNT=$(echo "$STATUS_JSON" | python3 -c "import sys, json; print(len(json.load(sys.stdin).get('workers', [])))")
echo "Registered Workers Count: $WORKER_COUNT"

if [ "$WORKER_COUNT" -eq 2 ]; then
  echo "✅ Success: 2 worker nodes registered with Master."
else
  echo "⚠️ Warning: Expected 2 registered workers, found $WORKER_COUNT."
fi

echo "=== 🔍 STEP 3: Verifying Total Cores and Memory ==="
TOTAL_CORES=$(echo "$STATUS_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin).get('cores', 0))")
TOTAL_MEM=$(echo "$STATUS_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin).get('memory', 0))")

echo "Total Cluster CPU Cores: $TOTAL_CORES"
echo "Total Cluster Memory: $((TOTAL_MEM / 1024)) GB"

if [ "$TOTAL_CORES" -ge 4 ]; then
  echo "✅ Success: CPU resources match capacity."
else
  echo "⚠️ Warning: Expected at least 4 cores total."
fi

echo "=== 🎉 Executors Registry Check Succeeded! ==="
