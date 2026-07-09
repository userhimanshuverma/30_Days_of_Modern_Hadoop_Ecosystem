#!/bin/bash
# Day 18: Verify HDFS Checkpoint Directory Script
# Location: Day-18-Spark-Structured-Streaming/scripts/verify-checkpoints.sh

set -e

CHECKPOINT_PATH="hdfs://namenode:9000/tmp/spark-checkpoints/clickstream"

echo "Checking Spark checkpoint directories on HDFS at: $CHECKPOINT_PATH"

# 1. Check if the root checkpoint directory exists in HDFS
if ! hadoop fs -test -d "$CHECKPOINT_PATH"; then
  echo "[X] Error: Checkpoint directory '$CHECKPOINT_PATH' does not exist yet."
  echo "    Ensure the streaming job has started and processed at least one micro-batch."
  exit 1
fi

echo "[✓] Checkpoint directory found."

# 2. List and explain subdirectories
echo "Inspecting Structured Streaming checkpoint subdirectories:"

# Helper to verify subfolder
check_subfolder() {
  local sub=$1
  local desc=$2
  if hadoop fs -test -d "$CHECKPOINT_PATH/$sub"; then
    echo "  - /$sub : [✓] Found. ($desc)"
  else
    echo "  - /$sub : [X] Missing. ($desc)"
  fi
}

check_subfolder "metadata" "Query Metadata containing the globally unique Run ID"
check_subfolder "offsets" "Source offsets corresponding to each micro-batch (wal)"
check_subfolder "commits" "Offsets committed by sinks; indicates processed batches"
check_subfolder "state" "Stateful store snapshots containing aggregated intermediate data"

echo "--------------------------------------------------------"
echo "Checkpoint metadata preview:"
hadoop fs -cat "$CHECKPOINT_PATH/metadata" 2>/dev/null || echo "Unable to read query metadata file."
echo ""
