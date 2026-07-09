#!/bin/bash
# Day 18: Verify Streaming Job Status Script
# Location: Day-18-Spark-Structured-Streaming/scripts/verify-streaming.sh

echo "Checking for active Spark Structured Streaming applications..."

# 1. Check if the PySpark process is running locally
SPARK_PID=$(pgrep -f "StreamingApp.py" || true)

if [ -z "$SPARK_PID" ]; then
  echo "[X] No active StreamingApp.py process found running on this container."
  echo "    To submit the streaming application, run:"
  echo "    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /workspace/source/StreamingApp.py &"
  exit 1
else
  echo "[✓] Found running StreamingApp.py process with PID: $SPARK_PID."
fi

# 2. Check if the Spark driver UI (default port 4040) is listening
if curl -s http://localhost:4040/ >/dev/null; then
  echo "[✓] Spark Driver Web UI is active and listening on port 4040."
  echo "    You can access the UI at http://localhost:4040 to view Active Streaming Queries."
else
  echo "[!] Spark Driver Web UI is not responsive on port 4040 yet (it may take a moment to initialize)."
fi

# 3. Check for the output query logs (if tailing logs)
echo "Streaming status checked successfully."
