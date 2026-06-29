#!/usr/bin/env bash
# ==============================================================================
# verify-resource-isolation.sh
# Verifies Linux CGroups mounting and YARN resource isolation parameters.
# ==============================================================================

set -eo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

echo "=========================================================="
echo "⚡ DIAGNOSTICS: Verifying CGroups Resource Isolation"
echo "=========================================================="

# 1. Check if cgroup fs is mounted
if mount | grep -q "cgroup"; then
    echo -e "${GREEN}[OK] CGroups file system is mounted.${NC}"
else
    echo -e "${RED}[ERROR] CGroups filesystem is not mounted. Check host virtualization settings.${NC}"
    exit 1
fi

# 2. Check for CPU and Memory controllers
controllers=("/sys/fs/cgroup/cpu" "/sys/fs/cgroup/memory" "/sys/fs/cgroup/cpuacct")
for controller in "${controllers[@]}"; do
    if [ -d "$controller" ]; then
        echo -e "${GREEN}[OK] Controller directory exists: $controller${NC}"
    else
        # For cgroups v2, directories might look different, check /sys/fs/cgroup/cgroup.controllers
        if [ -f "/sys/fs/cgroup/cgroup.controllers" ]; then
            echo -e "${GREEN}[OK] Controller $controller managed via CGroups v2 unified structure.${NC}"
        else
            echo -e "${RED}[WARNING] Controller directory not found: $controller${NC}"
        fi
    fi
done

# 3. Check for YARN sub-hierarchy
YARN_CGROUP="/sys/fs/cgroup/cpu/hadoop-yarn"
if [ -d "$YARN_CGROUP" ]; then
    echo -e "${GREEN}[OK] YARN CGroups hierarchy path exists: $YARN_CGROUP${NC}"
else
    echo -e "${GREEN}[INFO] YARN CGroups hierarchy ($YARN_CGROUP) is not initialized yet. NodeManager will generate it dynamically upon container launch.${NC}"
fi

echo "=========================================================="
echo -e "${GREEN}Resource isolation verification completed!${NC}"
echo "=========================================================="
