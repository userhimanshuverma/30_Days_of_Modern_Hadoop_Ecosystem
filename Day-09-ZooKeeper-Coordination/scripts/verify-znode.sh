#!/usr/bin/env bash
# verify-znode.sh
# Tests ZNode CRUD operations on the ZooKeeper ensemble using zkCli.sh.

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

TEST_NODE="/test-verification-node"
TEST_VAL="production-ready-zookeeper"
UPDATED_VAL="hadoop-ecosystem-day-09"

echo -e "${YELLOW}=== Testing ZNode Operations ===${NC}"

# Target Node 1 for writing
echo -e "\n1. Creating ZNode ${YELLOW}${TEST_NODE}${NC} on zookeeper1 with value: '${TEST_VAL}'..."
CREATE_OUT=$(docker exec zookeeper1 zkCli.sh -server localhost:2181 create "${TEST_NODE}" "${TEST_VAL}" 2>&1)

if echo "${CREATE_OUT}" | grep -q "Created ${TEST_NODE}"; then
  echo -e "${GREEN}[OK] ZNode created successfully.${NC}"
else
  echo -e "${RED}[ERROR] ZNode creation failed. Output:${NC}"
  echo "${CREATE_OUT}"
  exit 1
fi

# Target Node 2 for reading to verify cluster synchronization
echo -e "\n2. Reading ZNode ${YELLOW}${TEST_NODE}${NC} on zookeeper2 to verify replication..."
READ_OUT=$(docker exec zookeeper2 zkCli.sh -server localhost:2181 get "${TEST_NODE}" 2>&1)

if echo "${READ_OUT}" | grep -q "${TEST_VAL}"; then
  echo -e "${GREEN}[OK] Read successful. Data replicated to zookeeper2.${NC}"
else
  echo -e "${RED}[ERROR] Read failed or data mismatch on zookeeper2. Output:${NC}"
  echo "${READ_OUT}"
  exit 1
fi

# Target Node 3 for update
echo -e "\n3. Updating ZNode ${YELLOW}${TEST_NODE}${NC} on zookeeper3..."
UPDATE_OUT=$(docker exec zookeeper3 zkCli.sh -server localhost:2181 set "${TEST_NODE}" "${UPDATED_VAL}" 2>&1)

if echo "${UPDATE_OUT}" | grep -q "dataVersion"; then
  echo -e "${GREEN}[OK] ZNode updated successfully.${NC}"
else
  echo -e "${RED}[ERROR] ZNode update failed. Output:${NC}"
  echo "${UPDATE_OUT}"
  exit 1
fi

# Target Node 1 for verifying update
echo -e "\n4. Verifying updated value on zookeeper1..."
VERIFY_OUT=$(docker exec zookeeper1 zkCli.sh -server localhost:2181 get "${TEST_NODE}" 2>&1)

if echo "${VERIFY_OUT}" | grep -q "${UPDATED_VAL}"; then
  echo -e "${GREEN}[OK] Verification successful. ZNode updated value replicated.${NC}"
else
  echo -e "${RED}[ERROR] Updated value verify failed. Output:${NC}"
  echo "${VERIFY_OUT}"
  exit 1
fi

# Clean up / Delete ZNode
echo -e "\n5. Deleting ZNode ${YELLOW}${TEST_NODE}${NC} on zookeeper1..."
DELETE_OUT=$(docker exec zookeeper1 zkCli.sh -server localhost:2181 delete "${TEST_NODE}" 2>&1)

# Verify ZNode is deleted
echo -e "\n6. Checking if ZNode was deleted..."
CHECK_OUT=$(docker exec zookeeper2 zkCli.sh -server localhost:2181 get "${TEST_NODE}" 2>&1 || echo "Error")

if echo "${CHECK_OUT}" | grep -q "NoNodeException"; then
  echo -e "${GREEN}[OK] Clean-up successful. ZNode deleted across all nodes.${NC}"
  echo -e "\n${GREEN}[SUCCESS] ZNode CRUD operations verified successfully!${NC}"
  exit 0
else
  echo -e "${RED}[ERROR] Clean-up failed. ZNode still exists. Output:${NC}"
  echo "${CHECK_OUT}"
  exit 1
fi
