#!/bin/bash
# Day 18: Verify Windowed Output in Storage Script
# Location: Day-18-Spark-Structured-Streaming/scripts/verify-windowing.sh

set -e

OUTPUT_PATH="hdfs://namenode:9000/tmp/spark-outputs/clickstream"

echo "Checking streaming Parquet outputs on HDFS at: $OUTPUT_PATH"

# 1. Check if output directory exists
if ! hadoop fs -test -d "$OUTPUT_PATH"; then
  echo "[X] Error: Output directory '$OUTPUT_PATH' does not exist."
  echo "    Note: In 'Append' mode, the output is only written when the watermark passes the window end time."
  echo "    This means you must produce events with newer event_times to advance the watermark!"
  exit 1
fi

# 2. List the written Parquet partitions/files
echo "[✓] Output files found in storage:"
hadoop fs -ls -R "$OUTPUT_PATH" | grep "\.parquet" || echo "  (No parquet data files written yet; only directories or logs)"

# 3. Read and print the data contents using a quick PySpark read command
echo "--------------------------------------------------------"
echo "Reading written Parquet files via Spark Session to verify schema and data:"
echo "--------------------------------------------------------"

python3 -c "
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('VerifyParquetOutput').getOrCreate()

spark.sparkContext.setLogLevel('WARN')

try:
    df = spark.read.parquet('$OUTPUT_PATH')
    if df.count() == 0:
        print('Parquet schema exists, but contains 0 rows of data.')
    else:
        df.orderBy('window_start').show(20, truncate=False)
except Exception as e:
    print('Error reading parquet data:', e)
    sys.exit(1)
"
echo "--------------------------------------------------------"
echo "[✓] Validation complete."
