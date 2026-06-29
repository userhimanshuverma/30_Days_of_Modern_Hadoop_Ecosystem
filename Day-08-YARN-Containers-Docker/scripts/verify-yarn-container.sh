#!/usr/bin/env bash
# ==============================================================================
# verify-yarn-container.sh
# Checks YARN configs and validation of container-executor permissions.
# ==============================================================================

set -eo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

echo "=========================================================="
echo "⚡ DIAGNOSTICS: Verifying YARN Container Configurations"
echo "=========================================================="

CONFIG_FILE="/etc/hadoop/yarn-site.xml"
if [ ! -f "$CONFIG_FILE" ] && [ -f "../configs/yarn-site.xml" ]; then
    CONFIG_FILE="../configs/yarn-site.xml"
fi

echo "Analyzing configurations in: $CONFIG_FILE"

# 1. Check container-executor class
executor_class=$(grep -A 1 "yarn.nodemanager.container-executor.class" "$CONFIG_FILE" | grep "value" | sed 's/.*<value>\(.*\)<\/value>.*/\1/' || echo "Not Found")
echo "Container Executor Class: $executor_class"
if [[ "$executor_class" == *"LinuxContainerExecutor"* ]]; then
    echo -e "${GREEN}[OK] LinuxContainerExecutor is configured.${NC}"
else
    echo -e "${RED}[ERROR] LinuxContainerExecutor is NOT configured or missing in yarn-site.xml. Current: $executor_class${NC}"
fi

# 2. Check allowed runtimes
allowed_runtimes=$(grep -A 1 "yarn.nodemanager.runtime.linux.allowed-runtimes" "$CONFIG_FILE" | grep "value" | sed 's/.*<value>\(.*\)<\/value>.*/\1/' || echo "Not Found")
echo "Allowed Runtimes: $allowed_runtimes"
if [[ "$allowed_runtimes" == *"docker"* ]]; then
    echo -e "${GREEN}[OK] Docker runtime is enabled in allowed-runtimes.${NC}"
else
    echo -e "${RED}[ERROR] Docker runtime is missing in yarn.nodemanager.runtime.linux.allowed-runtimes.${NC}"
fi

# 3. Check container-executor binary permissions if running inside NodeManager container
CE_PATH="/usr/bin/container-executor"
if [ -f "$CE_PATH" ]; then
    owner=$(stat -c "%U" "$CE_PATH")
    group=$(stat -c "%G" "$CE_PATH")
    perms=$(stat -c "%a" "$CE_PATH")
    echo "container-executor binary info: owner=$owner, group=$group, perms=$perms"
    if [ "$owner" == "root" ] && [ "$perms" == "6050" ]; then
        echo -e "${GREEN}[OK] container-executor binary has secure permissions (owner: root, perms: 6050).${NC}"
    else
        echo -e "${RED}[WARNING] container-executor permissions should be 6050 and owned by root. Actual owner: $owner, perms: $perms${NC}"
    fi
else
    echo -e "${GREEN}[INFO] container-executor not found at local path $CE_PATH (Normal if running client-side).${NC}"
fi

echo "=========================================================="
echo -e "${GREEN}YARN configuration verification completed!${NC}"
echo "=========================================================="
