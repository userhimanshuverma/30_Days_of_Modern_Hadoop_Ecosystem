#!/usr/bin/env bash
# ==============================================================================
# verify-gcs.sh: Validation script for Google Cloud Storage (GCS) Connector
# ==============================================================================

set -eo pipefail

COLOR_GREEN='\033[0;32m'
COLOR_RED='\033[0;31m'
COLOR_BLUE='\033[0;34m'
COLOR_RESET='\033[0m'

echo -e "${COLOR_BLUE}=====================================================${COLOR_RESET}"
echo -e "${COLOR_BLUE}   DAY 29: Google Cloud Storage (GCS) Verification   ${COLOR_RESET}"
echo -e "${COLOR_BLUE}=====================================================${COLOR_RESET}"

GCS_BUCKET="${1:-gs://hadoop-hybrid-bucket}"
TEST_DIR="${GCS_BUCKET}/gcs_validation_$(date +%s)"
TEST_FILE="gcs_test_payload.txt"

echo "Creating payload..."
echo "Day 29 - GCS Connector Verification Payload: $(date)" > "${TEST_FILE}"

echo -e "\n1. Checking GCS Connector Filesystem (gs://)..."
if hdfs dfs -ls "${GCS_BUCKET}" > /dev/null 2>&1; then
    echo -e "${COLOR_GREEN}[SUCCESS] GCS Bucket accessible via GoogleHadoopFileSystem driver.${COLOR_RESET}"
else
    echo -e "${COLOR_RED}[WARNING/FAILED] Could not access GCS bucket: ${GCS_BUCKET}.${COLOR_RESET}"
    echo "Note: Ensure fs.gs.project.id and fs.gs.auth.service.account.json.keyfile are configured."
    exit 1
fi

echo -e "\n2. Writing to GCS Bucket..."
if hdfs dfs -mkdir -p "${TEST_DIR}" && hdfs dfs -put "${TEST_FILE}" "${TEST_DIR}/"; then
    echo -e "${COLOR_GREEN}[SUCCESS] Successfully created prefix and uploaded blob to GCS.${COLOR_RESET}"
else
    echo -e "${COLOR_RED}[FAILED] GCS Put operation failed.${COLOR_RESET}"
    exit 1
fi

echo -e "\n3. Reading Content Back from GCS..."
CONTENT=$(hdfs dfs -cat "${TEST_DIR}/${TEST_FILE}")
echo "Payload Read back: ${CONTENT}"
if [[ "${CONTENT}" == *"Day 29"* ]]; then
    echo -e "${COLOR_GREEN}[SUCCESS] Content verification passed.${COLOR_RESET}"
else
    echo -e "${COLOR_RED}[FAILED] Content mismatch.${COLOR_RESET}"
    exit 1
fi

echo -e "\n4. Cleaning up..."
hdfs dfs -rm -r -skipTrash "${TEST_DIR}"
rm -f "${TEST_FILE}"

echo -e "\n${COLOR_GREEN}>>> GCS Integration Verification Complete: PASSED <<<${COLOR_RESET}\n"
