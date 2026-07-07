#!/bin/bash
# Day 16: Spark Standalone Cluster Verification Script
# Location: Day-16-Spark-Core-Architecture/scripts/verify-spark-cluster.sh

set -e

echo "=== 🔍 STEP 1: Probing Spark Master Daemon Ports ==="
# Check Master RPC port 7077
if timeout 3 bash -c '</dev/tcp/spark-master/7077' 2>/dev/null; then
  echo "✅ Success: Spark Master RPC port 7077 is listening."
else
  echo "❌ Error: Spark Master RPC port 7077 is unreachable."
  exit 1
fi

# Check Master Web UI port 8080
if curl -s -f http://spark-master:8080/ > /dev/null; then
  echo "✅ Success: Spark Master Web UI port 8080 is active."
else
  echo "❌ Error: Spark Master Web UI port 8080 is offline."
  exit 1
fi

echo "=== 🔍 STEP 2: Probing Spark Worker Web UIs ==="
# Check Worker 1 UI port 8081
if curl -s -f http://spark-worker-1:8081/ > /dev/null; then
  echo "✅ Success: Spark Worker-1 Web UI port 8081 is active."
else
  echo "❌ Error: Spark Worker-1 Web UI is offline."
  exit 1
fi

# Check Worker 2 UI port 8081
if curl -s -f http://spark-worker-2:8081/ > /dev/null; then
  echo "✅ Success: Spark Worker-2 Web UI port 8081 is active."
else
  echo "❌ Error: Spark Worker-2 Web UI is offline."
  exit 1
fi

echo "=== 🎉 Spark Cluster Daemons Verified Successfully! ==="
