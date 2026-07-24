#!/usr/bin/env bash
# ==============================================================================
# verify-s3.sh: Validation script for S3A / S3-compatible Object Storage Access
# ==============================================================================

set -eo pipefail

COLOR_GREEN='\033[0;32m'
COLOR_RED='\033[0;31m'
COLOR_BLUE='\033[0;34m'
COLOR_RESET='\033[0m'

echo -e "${COLOR_BLUE}=====================================================${COLOR_RESET}"
echo -e "${COLOR_BLUE}   DAY 29: AWS S3 / MinIO Integration Verification   ${COLOR_RESET}"
echo -e "${COLOR_BLUE}=====================================================${COLOR_RESET}"

TARGET_BUCKET="${1:-s3a://warehouse}"
TEST_DIR="${TARGET_BUCKET}/validation_test_$(date +%s)"
TEST_FILE="s3_test_payload.txt"

echo "Creating test file..."
echo "Day 29 - S3A Object Storage Verification Payload: $(date)" > "${TEST_FILE}"

echo -e "\n1. Testing S3 Bucket Listing (${TARGET_BUCKET})..."
if hdfs dfs -ls "${TARGET_BUCKET}" > /dev/null 2>&1; then
    echo -e "${COLOR_GREEN}[SUCCESS] S3 Bucket exists and is readable.${COLOR_RESET}"
else
    echo -e "${COLOR_RED}[FAILED] Unable to access S3 bucket: ${TARGET_BUCKET}${COLOR_RESET}"
    exit 1
fi

echo -e "\n2. Testing Write Operation to S3A (${TEST_DIR})..."
if hdfs dfs -mkdir -p "${TEST_DIR}" && hdfs dfs -put "${TEST_FILE}" "${TEST_DIR}/"; then
    echo -e "${COLOR_GREEN}[SUCCESS] Successfully wrote test payload to S3A.${COLOR_RESET}"
else
    echo -e "${COLOR_RED}[FAILED] Failed to upload test file to S3A.${COLOR_RESET}"
    exit 1
fi

echo -e "\n3. Testing Read Operation from S3A..."
READ_CONTENT=$(hdfs dfs -cat "${TEST_DIR}/${TEST_FILE}")
echo "Payload Read back: ${READ_CONTENT}"
if [[ "${READ_CONTENT}" == *"Day 29"* ]]; then
    echo -e "${COLOR_GREEN}[SUCCESS] Payload content matched perfectly.${COLOR_RESET}"
else
    echo -e "${COLOR_RED}[FAILED] Payload mismatch or read error.${COLOR_RESET}"
    exit 1
fi

echo -e "\n4. Cleaning up test artifacts..."
hdfs dfs -rm -r -skipTrash "${TEST_DIR}"
rm -f "${TEST_FILE}"

echo -e "\n${COLOR_GREEN}>>> S3A Integration Verification Complete: PASSED <<<${COLOR_RESET}\n"
