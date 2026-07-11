#!/bin/bash
# Day 20: Apache Hive Metastore Verification Script
# Location: Day-20-Hive-Architecture-HMS/scripts/verify-metastore.sh

set -e

HMS_HOST=${1:-"hive-metastore"}
HMS_PORT=9083

echo "=== 🔍 STEP 1: Probing Hive Metastore thrift port ($HMS_HOST:$HMS_PORT) ==="
if nc -z "$HMS_HOST" "$HMS_PORT"; then
  echo "✅ Success: Hive Metastore is listening on port $HMS_PORT."
else
  echo "❌ Error: Hive Metastore is offline or unreachable on $HMS_HOST:$HMS_PORT."
  exit 1
fi

echo "=== 🔍 STEP 2: Verifying PostgreSQL Schema status ==="
# We can check schema availability using schematool info
if /opt/hive/bin/schematool -dbType postgres -info > /dev/null 2>&1; then
  echo "✅ Success: Hive Metastore schema is active and validated in PostgreSQL."
else
  echo "❌ Error: Hive Metastore database connection failed or schema is uninitialized."
  exit 1
fi

echo "=== 🎉 Hive Metastore Service (HMS) Verification Passed! ==="
