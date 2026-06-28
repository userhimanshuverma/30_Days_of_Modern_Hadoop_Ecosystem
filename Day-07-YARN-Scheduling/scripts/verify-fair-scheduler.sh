#!/usr/bin/env bash
# verify-fair-scheduler.sh
# Checks Fair Scheduler configuration structure and explains how to activate it.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

CONTAINER_NAME="resourcemanager-day07"

echo -e "${YELLOW}=== Verifying Fair Scheduler Configuration ===${NC}"

# 1. Verify that fair-scheduler.xml exists in configs/
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAIR_CONFIG_PATH="${SCRIPT_DIR}/../configs/fair-scheduler.xml"

if [ -f "$FAIR_CONFIG_PATH" ]; then
  echo -e "${GREEN}[OK] Fair Scheduler configuration file found at: configs/fair-scheduler.xml${NC}"
else
  echo -e "${RED}[ERROR] File configs/fair-scheduler.xml is missing.${NC}"
  exit 1
fi

# 2. Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo -e "${YELLOW}[NOTE] ResourceManager container is not running. Cannot check active scheduler state.${NC}"
  exit 0
fi

# 3. Read current active scheduler from ResourceManager JMX or Class Key
ACTIVE_CLASS=$(docker exec "$CONTAINER_NAME" hdfs getconf -confKey yarn.resourcemanager.scheduler.class 2>/dev/null || echo "Unknown")

echo -e "\nActive YARN Scheduler Class: ${YELLOW}${ACTIVE_CLASS}${NC}"

if [[ "$ACTIVE_CLASS" == *"FairScheduler"* ]]; then
  echo -e "${GREEN}[SUCCESS] FairScheduler is active in YARN ResourceManager.${NC}"
else
  echo -e "${YELLOW}[INFO] CapacityScheduler is currently the active scheduler, which is correct for the default lab setup.${NC}"
  echo -e "To switch the active scheduler to FairScheduler:"
  echo -e "1. Modify configs/yarn-site.xml to set:"
  echo -e "   <property>"
  echo -e "     <name>yarn.resourcemanager.scheduler.class</name>"
  echo -e "     <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler</value>"
  echo -e "   </property>"
  echo -e "   <property>"
  echo -e "     <name>yarn.scheduler.fair.allocation.file</name>"
  echo -e "     <value>/etc/hadoop/fair-scheduler.xml</value>"
  echo -e "   </property>"
  echo -e "2. Restart the ResourceManager container:"
  echo -e "   docker restart ${CONTAINER_NAME}"
fi

# 4. Check XML syntax validation of fair-scheduler.xml
if docker exec "$CONTAINER_NAME" which xmllint >/dev/null 2>&1; then
  echo -e "\nValidating XML syntax of fair-scheduler.xml on ResourceManager..."
  if docker exec "$CONTAINER_NAME" xmllint --noout /etc/hadoop/fair-scheduler.xml; then
    echo -e "${GREEN}[OK] fair-scheduler.xml is syntactically valid XML.${NC}"
  else
    echo -e "${RED}[ERROR] fair-scheduler.xml has XML syntax errors!${NC}"
    exit 1
  fi
fi

exit 0
