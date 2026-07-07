#!/bin/bash
# Day 16: Spark Driver Verification Script
# Location: Day-16-Spark-Core-Architecture/scripts/verify-driver.sh

set -e

echo "=== 🔍 STEP 1: Sourcing Environment Configs ==="
if [ -f "/opt/spark/conf/spark-env.sh" ]; then
  source /opt/spark/conf/spark-env.sh
  echo "✅ Success: Sourced spark-env.sh."
else
  echo "❌ Error: spark-env.sh is missing from /opt/spark/conf/."
  exit 1
fi

echo "=== 🔍 STEP 2: Checking spark-submit Utility ==="
if command -v spark-submit &> /dev/null; then
  echo "✅ Success: spark-submit tool is available in the system PATH."
  spark-submit --version 2>&1 | grep "version"
else
  echo "❌ Error: spark-submit utility not found in path."
  exit 1
fi

echo "=== 🔍 STEP 3: Checking Local Directory Permissions ==="
if [ -w "/opt/spark/work" ]; then
  echo "✅ Success: Work directory /opt/spark/work is writeable."
else
  echo "❌ Error: Work directory is not writeable."
  exit 1
fi

echo "=== 🎉 Spark Driver Environment Succeeded! ==="
