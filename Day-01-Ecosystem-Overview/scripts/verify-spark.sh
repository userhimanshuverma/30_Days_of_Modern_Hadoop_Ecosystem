#!/usr/bin/env bash
# =========================================================================
# Spark Processing Cluster Validation Script - Day 1
# Submits a test PySpark application to verify master-worker execution.
# =========================================================================

set -euo pipefail

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

MASTER_CONTAINER="spark-master-day01"
WORKER_CONTAINER="spark-worker-day01"
TEST_SCRIPT="temp_spark_test.py"

echo -e "${YELLOW}=== Starting Spark Cluster Validation ===${NC}"

# 1. Check if Master and Worker Containers are Running
if ! docker ps --filter "name=${MASTER_CONTAINER}" --filter "status=running" | grep -q "${MASTER_CONTAINER}"; then
  echo -e "${RED}[ERROR] Spark Master container '${MASTER_CONTAINER}' is not running.${NC}"
  exit 1
fi
if ! docker ps --filter "name=${WORKER_CONTAINER}" --filter "status=running" | grep -q "${WORKER_CONTAINER}"; then
  echo -e "${RED}[ERROR] Spark Worker container '${WORKER_CONTAINER}' is not running.${NC}"
  exit 1
fi
echo -e "${GREEN}[OK] Spark Master and Worker containers are running.${NC}"

# 2. Generate a PySpark Test Script
cat << 'EOF' > "${TEST_SCRIPT}"
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SparkValidationTest") \
        .getOrCreate()

    # Create a small dataset and perform a basic operation
    data = [("Hadoop", 1), ("Spark", 2), ("Hive", 3), ("Kafka", 4), ("ZooKeeper", 5)]
    df = spark.createDataFrame(data, ["component", "day"])
    
    count = df.count()
    print(f"VALIDATION_OUTPUT: SUCCESS - Read {count} records in Spark.")
    
    spark.stop()
EOF

# 3. Copy Script to Master Container
echo "Copying Spark test script to Master..."
docker cp "${TEST_SCRIPT}" "${MASTER_CONTAINER}:/tmp/${TEST_SCRIPT}"

# 4. Submit Job to Spark Cluster
echo "Submitting PySpark application to cluster master (spark://spark-master-day01:7077)..."
JOB_OUTPUT=$(docker exec "${MASTER_CONTAINER}" spark-submit \
  --master spark://spark-master-day01:7077 \
  /tmp/${TEST_SCRIPT} 2>&1)

# 5. Verify Success
if echo "$JOB_OUTPUT" | grep -q "VALIDATION_OUTPUT: SUCCESS"; then
  echo -e "${GREEN}[OK] Spark Job executed successfully on Spark Master and Worker!${NC}"
  echo "$JOB_OUTPUT" | grep "VALIDATION_OUTPUT"
else
  echo -e "${RED}[ERROR] Spark Job execution failed.${NC}"
  echo -e "${YELLOW}--- JOB OUTPUT ---${NC}"
  echo "$JOB_OUTPUT"
  echo -e "${YELLOW}------------------${NC}"
  exit 1
fi

# 6. Cleanup
echo "Cleaning up local and container test files..."
docker exec "${MASTER_CONTAINER}" rm -f "/tmp/${TEST_SCRIPT}"
rm -f "${TEST_SCRIPT}"

echo -e "${GREEN}=== Spark Cluster Validation PASSED successfully! ===${NC}"
