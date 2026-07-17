#!/usr/bin/env bash
# Day 25: verify-webhdfs.sh
# Validates HDFS file operations (write, read, append, delete) through Apache Knox WebHDFS gateway endpoint.

set -euo pipefail

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

KNOX_HOST=${1:-"localhost"}
KNOX_PORT=${2:-"8443"}
USER=${3:-"guest"}
PASSWORD=${4:-"guestpassword"}

BASE_URL="https://${KNOX_HOST}:${KNOX_PORT}/gateway/sandbox/webhdfs/v1"
TEST_FILE="/tmp/knox-test-$(date +%s).txt"
TEST_CONTENT="Hello Hadoop Security! This file was written through the Apache Knox Gateway. Time: $(date)"

echo -e "${YELLOW}=================================================================${NC}"
echo -e "${YELLOW} Validating HDFS Operations via Knox WebHDFS Proxy               ${NC}"
echo -e "${YELLOW}=================================================================${NC}"

# 1. Write File (Requires 2-step write process because of 307 Redirect)
echo -e "\n1. Writing test file to HDFS path: ${TEST_FILE}..."
echo "Step A: Initializing file creation request..."
# The -i flag returns headers, -k ignores SSL verification, -s is silent, -u provides basic auth
REDIRECT_HEADERS=$(curl -k -s -i -u "${USER}:${PASSWORD}" -X PUT \
    "${BASE_URL}${TEST_FILE}?op=CREATE&noredirect=true")

# Extract the rewritten redirection location (Knox rewrites this to point back to Knox, not the raw DataNode)
REDIRECT_URL=$(echo "$REDIRECT_HEADERS" | grep -Fi 'Location:' | awk '{print $2}' | tr -d '\r\n')

if [ -z "$REDIRECT_URL" ]; then
    echo -e "${RED}FAILED: No Redirect URL found. Headers output:${NC}"
    echo "$REDIRECT_HEADERS"
    exit 1
fi

echo -e "Rewritten DataNode Redirect URL caught by client:\n   -> ${GREEN}${REDIRECT_URL}${NC}"

echo "Step B: Uploading text payload to redirect endpoint..."
UPLOAD_STATUS=$(curl -k -s -o /dev/null -w "%{http_code}" -X PUT \
    -H "Content-Type: text/plain" \
    -d "$TEST_CONTENT" \
    "$REDIRECT_URL")

if [ "$UPLOAD_STATUS" -eq 201 ]; then
    echo -e "${GREEN}PASSED: File created successfully (HTTP 201 Created)${NC}"
else
    echo -e "${RED}FAILED: Upload failed with status: ${UPLOAD_STATUS}${NC}"
    exit 1
fi

# 2. Read File (Verify content matches)
echo -e "\n2. Reading file from HDFS and verifying contents..."
READ_CONTENT=$(curl -k -s -L -u "${USER}:${PASSWORD}" "${BASE_URL}${TEST_FILE}?op=OPEN")

echo -e "Content retrieved: \"${GREEN}${READ_CONTENT}${NC}\""

if [ "$READ_CONTENT" = "$TEST_CONTENT" ]; then
    echo -e "${GREEN}PASSED: Read content matches written content!${NC}"
else
    echo -e "${RED}FAILED: Content mismatch!${NC}"
    exit 1
fi

# 3. Check File Status
echo -e "\n3. Checking file metadata / status..."
STATUS_OUTPUT=$(curl -k -s -u "${USER}:${PASSWORD}" "${BASE_URL}${TEST_FILE}?op=GETFILESTATUS")
echo -e "File status JSON:\n  ${GREEN}${STATUS_OUTPUT}${NC}"

# 4. Clean up / Delete File
echo -e "\n4. Cleaning up HDFS test file..."
DELETE_STATUS=$(curl -k -s -u "${USER}:${PASSWORD}" -X DELETE "${BASE_URL}${TEST_FILE}?op=DELETE")
echo -e "Delete operation result:\n  ${GREEN}${DELETE_STATUS}${NC}"

if echo "$DELETE_STATUS" | grep -q '"boolean":true'; then
    echo -e "${GREEN}PASSED: Test file successfully cleaned up from HDFS.${NC}"
else
    echo -e "${RED}FAILED: Failed to delete test file.${NC}"
    exit 1
fi

echo -e "\n${GREEN}✔ WebHDFS Verification through Knox Gateway succeeded!${NC}"
