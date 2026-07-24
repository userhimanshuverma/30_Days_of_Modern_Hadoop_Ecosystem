#!/usr/bin/env bash
# ==============================================================================
# verify-spark.sh: Validation script for Apache Spark writing/reading Object Storage
# ==============================================================================

set -eo pipefail

COLOR_GREEN='\033[0;32m'
COLOR_RED='\033[0;31m'
COLOR_BLUE='\033[0;34m'
COLOR_RESET='\033[0m'

echo -e "${COLOR_BLUE}=====================================================${COLOR_RESET}"
echo -e "${COLOR_BLUE}   DAY 29: Apache Spark Object Storage Verification   ${COLOR_RESET}"
echo -e "${COLOR_BLUE}=====================================================${COLOR_RESET}"

SPARK_SUBMIT="${SPARK_HOME:-/opt/bitnami/spark}/bin/spark-submit"

PYSPARK_SCRIPT="/tmp/spark_cloud_test.py"
cat << 'EOF' > "${PYSPARK_SCRIPT}"
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName("Day29-SparkCloudValidation") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadminpassword") \
    .getOrCreate()

data = [("event_101", "user_42", "purchase", 199.99), ("event_102", "user_99", "click", 0.0)]
df = spark.createDataFrame(data, ["event_id", "user_id", "action", "amount"])

output_path = "s3a://warehouse/spark_test_output"

print("Writing DataFrame to S3A in Parquet format...")
df.write.mode("overwrite").parquet(output_path)

print("Reading back DataFrame from S3A...")
read_df = spark.read.parquet(output_path)
count = read_df.count()

print(f"Read back count: {count}")
assert count == 2, f"Expected 2 rows, got {count}"

print("Spark Object Storage Integration Verified Successfully!")
spark.stop()
EOF

echo "Running Spark job to write Parquet files to s3a://..."
if ${SPARK_SUBMIT} --master local[2] "${PYSPARK_SCRIPT}"; then
    echo -e "${COLOR_GREEN}[SUCCESS] Spark successfully executed read/write cycle against object storage.${COLOR_RESET}"
else
    echo -e "${COLOR_RED}[FAILED] Spark job failed during cloud object storage execution.${COLOR_RESET}"
    rm -f "${PYSPARK_SCRIPT}"
    exit 1
fi

rm -f "${PYSPARK_SCRIPT}"
echo -e "\n${COLOR_GREEN}>>> Spark Object Storage Verification Complete: PASSED <<<${COLOR_RESET}\n"
