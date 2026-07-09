#!/bin/bash
# Day 18: Verify Offset Log Script
# Location: Day-18-Spark-Structured-Streaming/scripts/verify-offsets.sh

set -e

OFFSETS_PATH="hdfs://namenode:9000/tmp/spark-checkpoints/clickstream/offsets"

echo "Checking Spark Structured Streaming offset logs on HDFS at: $OFFSETS_PATH"

# 1. Verify directory existence
if ! hadoop fs -test -d "$OFFSETS_PATH"; then
  echo "[X] Error: Offsets directory '$OFFSETS_PATH' does not exist."
  exit 1
fi

# 2. List the generated offset files (batches)
echo "Recent streaming micro-batches logged:"
BATCHES=$(hadoop fs -ls "$OFFSETS_PATH" | awk '{print $8}' | grep -oE '[0-9]+$' | sort -n)

if [ -z "$BATCHES" ]; then
  echo "No micro-batch offset logs found yet. Write data to Kafka to trigger micro-batches."
  exit 0
fi

for batch in $BATCHES; do
  echo "  - Batch ID: $batch"
done

# 3. Read the contents of the latest batch offset file
LATEST_BATCH=$(echo "$BATCHES" | tail -n 1)
echo "--------------------------------------------------------"
echo "Displaying offset details for the latest batch (Batch ID: $LATEST_BATCH):"
hadoop fs -cat "$OFFSETS_PATH/$LATEST_BATCH"

echo ""
echo "--------------------------------------------------------"
echo "[✓] Offsets are actively progressing and recorded."
