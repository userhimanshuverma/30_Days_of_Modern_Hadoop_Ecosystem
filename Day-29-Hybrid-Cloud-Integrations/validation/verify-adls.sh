#!/usr/bin/env bash
# ==============================================================================
# verify-adls.sh: Validation script for Azure Data Lake Storage Gen2 (ABFS)
# ==============================================================================

set -eo pipefail

COLOR_GREEN='\033[0;32m'
COLOR_RED='\033[0;31m'
COLOR_BLUE='\033[0;34m'
COLOR_RESET='\033[0m'

echo -e "${COLOR_BLUE}=====================================================${COLOR_RESET}"
echo -e "${COLOR_BLUE}   DAY 29: Azure ADLS Gen2 Integration Verification  ${COLOR_RESET}"
echo -e "${COLOR_BLUE}=====================================================${COLOR_RESET}"

CONTAINER_URL="${1:-abfs://container-name@account-name.dfs.core.windows.net}"
TEST_DIR="${CONTAINER_URL}/adls_validation_$(date +%s)"
TEST_FILE="adls_test_payload.txt"

echo "Creating local payload..."
echo "Day 29 - ABFS Driver ADLS Gen2 Payload: $(date)" > "${TEST_FILE}"

echo -e "\n1. Checking ABFS Driver Filesystem Initialization..."
if hdfs dfs -ls "${CONTAINER_URL}" > /dev/null 2>&1; then
    echo -e "${COLOR_GREEN}[SUCCESS] ABFS container reached and authenticated via OAuth/Service Principal.${COLOR_RESET}"
else
    echo -e "${COLOR_RED}[WARNING/FAILED] Could not access ADLS Gen2 URL: ${CONTAINER_URL}.${COLOR_RESET}"
    echo "Note: Ensure Azure Client Secret, Tenant ID, and Storage Account parameters are filled in core-site.xml."
    exit 1
fi

echo -e "\n2. Testing Write to ADLS Gen2..."
if hdfs dfs -mkdir -p "${TEST_DIR}" && hdfs dfs -put "${TEST_FILE}" "${TEST_DIR}/"; then
    echo -e "${COLOR_GREEN}[SUCCESS] Successfully wrote object to ADLS Gen2 container.${COLOR_RESET}"
else
    echo -e "${COLOR_RED}[FAILED] Write operation to ADLS Gen2 failed.${COLOR_RESET}"
    exit 1
fi

echo -e "\n3. Testing Atomic Directory Rename (ADLS Gen2 HNS Feature)..."
RENAMED_DIR="${CONTAINER_URL}/adls_renamed_$(date +%s)"
if hdfs dfs -mv "${TEST_DIR}" "${RENAMED_DIR}"; then
    echo -e "${COLOR_GREEN}[SUCCESS] Atomic directory rename succeeded (Hierarchical Namespace active).${COLOR_RESET}"
else
    echo -e "${COLOR_RED}[FAILED] Directory rename operation failed.${COLOR_RESET}"
    exit 1
fi

echo -e "\n4. Cleaning up test artifacts..."
hdfs dfs -rm -r -skipTrash "${RENAMED_DIR}"
rm -f "${TEST_FILE}"

echo -e "\n${COLOR_GREEN}>>> ADLS Gen2 Integration Verification Complete: PASSED <<<${COLOR_RESET}\n"
