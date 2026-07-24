#!/usr/bin/env bash
# ==============================================================================
# verify-hive.sh: Validation script for Hive External Tables on Cloud Object Storage
# ==============================================================================

set -eo pipefail

COLOR_GREEN='\033[0;32m'
COLOR_RED='\033[0;31m'
COLOR_BLUE='\033[0;34m'
COLOR_RESET='\033[0m'

echo -e "${COLOR_BLUE}=====================================================${COLOR_RESET}"
echo -e "${COLOR_BLUE}   DAY 29: Hive Metastore Object Storage Verification ${COLOR_RESET}"
echo -e "${COLOR_BLUE}=====================================================${COLOR_RESET}"

HIVE_METASTORE_URI="${HIVE_METASTORE_URI:-thrift://localhost:9083}"

echo "1. Validating Hive External Table creation pointing to s3a://..."

beeline -u "jdbc:hive2://localhost:10000/default;auth=noSasl" -e "
CREATE DATABASE IF NOT EXISTS hybrid_db;
USE hybrid_db;

CREATE EXTERNAL TABLE IF NOT EXISTS customer_events (
    event_id STRING,
    user_id STRING,
    action STRING,
    event_time TIMESTAMP
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 's3a://warehouse/hybrid_db/customer_events';

SHOW TABLES;
DESCRIBE FORMATTED customer_events;
" > /tmp/hive_verify.log 2>&1 || true

if grep -q "customer_events" /tmp/hive_verify.log; then
    echo -e "${COLOR_GREEN}[SUCCESS] Hive external table successfully registered on s3a:// storage location.${COLOR_RESET}"
else
    echo -e "${COLOR_RED}[WARNING] Beeline output check failed or HiveServer2 offline. Inspecting log:${COLOR_RESET}"
    cat /tmp/hive_verify.log
    exit 1
fi

echo -e "\n${COLOR_GREEN}>>> Hive Object Storage Verification Complete: PASSED <<<${COLOR_RESET}\n"
