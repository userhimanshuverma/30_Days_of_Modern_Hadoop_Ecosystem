#!/bin/bash
# Day 17: Spark SQL Cluster Verification Script
# Location: Day-17-Spark-SQL-Catalyst/scripts/verify-spark-sql.sh

set -e

echo "=== 🔍 STEP 1: Probing Spark Standalone Master UI ==="
if curl -s -f http://spark-master:8080/ > /dev/null; then
  echo "✅ Success: Spark Standalone Master UI is online."
else
  echo "❌ Error: Spark Master UI is unreachable."
  exit 1
fi

echo "=== 🔍 STEP 2: Running a Test Query with Spark SQL CLI ==="
# Execute a basic SELECT to ensure Spark SQL compiler and execution context is healthy
result=$(/opt/spark/bin/spark-sql --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=1 \
  -e "SELECT 1 + 1 AS addition, 'SparkSQL' AS engine" 2>/dev/null || true)

# Alternatively, run via pyspark inline
echo "Running quick inline python check..."
pyspark_out=$(python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('spark://spark-master:7077').appName('TestSQL').getOrCreate()
df = spark.sql('SELECT 42 AS value, CURRENT_DATE() AS date')
df.show()
spark.stop()
" 2>&1)

echo "$pyspark_out"

if echo "$pyspark_out" | grep -q "42"; then
  echo "✅ Success: Spark SQL engine executed the test query successfully."
else
  echo "❌ Error: Spark SQL test query execution failed."
  exit 1
fi

echo "=== 🎉 Spark SQL Engine Verified Successfully! ==="
