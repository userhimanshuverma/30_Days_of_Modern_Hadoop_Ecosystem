#!/bin/bash
# Day 20: HiveServer2 (HS2) Verification Script
# Location: Day-20-Hive-Architecture-HMS/scripts/verify-hiveserver2.sh

set -e

HS2_HOST=${1:-"localhost"}
HS2_PORT=10000

echo "=== 🔍 STEP 1: Probing HiveServer2 JDBC thrift port ($HS2_HOST:$HS2_PORT) ==="
if nc -z "$HS2_HOST" "$HS2_PORT"; then
  echo "✅ Success: HiveServer2 is listening on port $HS2_PORT."
else
  echo "❌ Error: HiveServer2 is offline or unreachable on $HS2_HOST:$HS2_PORT."
  exit 1
fi

echo "=== 🔍 STEP 2: Running a basic query via Beeline client ==="
# Execute a test select query using Beeline
if /opt/hive/bin/beeline -u "jdbc:hive2://$HS2_HOST:$HS2_PORT" -n hive -p hive -e "SELECT 'HiveServer2 Connection OK' AS status;" > /tmp/beeline_test.log 2>&1; then
  echo "✅ Success: Successfully connected to HiveServer2 and executed SQL query."
  cat /tmp/beeline_test.log | grep -A 2 -B 2 "status" || cat /tmp/beeline_test.log
else
  echo "❌ Error: Beeline connection to HiveServer2 failed."
  cat /tmp/beeline_test.log
  exit 1
fi

echo "=== 🎉 HiveServer2 (HS2) Connection Verification Passed! ==="
