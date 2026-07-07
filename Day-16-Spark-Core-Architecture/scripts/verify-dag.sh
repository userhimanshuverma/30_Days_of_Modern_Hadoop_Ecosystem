#!/bin/bash
# Day 16: Spark DAG & Event Log Verification Script
# Location: Day-16-Spark-Core-Architecture/scripts/verify-dag.sh

set -e

echo "=== 🔍 STEP 1: Verifying Spark Event Log Path in HDFS ==="
if /opt/hadoop/bin/hadoop fs -test -d /shared/spark-logs; then
  echo "✅ Success: Event logs directory /shared/spark-logs exists in HDFS."
else
  echo "❌ Error: Event logs directory /shared/spark-logs is missing from HDFS."
  exit 1
fi

echo "=== 🔍 STEP 2: Verifying Generated Event Log Files ==="
LOG_FILES=$(/opt/hadoop/bin/hadoop fs -ls /shared/spark-logs | grep -v "Found " | awk '{print $8}')

if [ -z "$LOG_FILES" ]; then
  echo "⚠️ Warning: No Spark event log files found. Run the Spark demo app first."
  exit 0
else
  echo "Event logs found:"
  echo "$LOG_FILES"
fi

echo "=== 🔍 STEP 3: Scanning Logs for DAG and Spark Events ==="
for log in $LOG_FILES; do
  echo "Scanning log file: $log"
  # Read event log (which is JSON lines format) and grep for Spark event types
  /opt/hadoop/bin/hadoop fs -cat "$log" | grep -E "SparkListenerJobStart|SparkListenerStageSubmitted|SparkListenerTaskEnd" | head -n 5
done

echo "=== 🎉 DAG & Event Log Verification Complete! ==="
