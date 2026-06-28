#!/usr/bin/env bash
# verify-resource-allocation.sh
# Analyzes node capacity and resource allocation ratios across multiple NodeManagers.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

CONTAINER_NAME="resourcemanager-day07"

echo -e "${YELLOW}=== YARN Cluster Node Resource Allocations ===${NC}"

# 1. Verify container status
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo -e "${RED}[ERROR] Container '${CONTAINER_NAME}' is not running.${NC}"
  exit 1
fi

# 2. Query ResourceManager Nodes REST Endpoint
NODES_URL="http://localhost:8088/ws/v1/cluster/nodes"
RESPONSE=$(curl -s -f --connect-timeout 5 "$NODES_URL" || echo "")

if [ -z "$RESPONSE" ]; then
  echo -e "${RED}[ERROR] Failed to query Nodes REST API. Cluster might still be starting up.${NC}"
  exit 1
fi

# 3. Parse node allocations using Python helper
python3 -c '
import sys, json

try:
    data = json.loads(sys.argv[1])
except Exception as e:
    print("Error parsing JSON:", e)
    sys.exit(1)

nodes = data.get("nodes", {}).get("node", [])
if not nodes:
    print("\033[1;31mNo NodeManagers registered with the ResourceManager yet!\033[0m")
    sys.exit(0)

print(f"Total Registered NodeManagers: {len(nodes)}\n")

for node in nodes:
    node_id = node.get("id", "N/A")
    host = node.get("nodeHostName", "N/A")
    state = node.get("state", "UNKNOWN")
    containers = node.get("numContainers", 0)
    
    # Available/Used resources
    avail_mem = node.get("availMemoryMB", 0)
    used_mem = node.get("usedMemoryMB", 0)
    avail_cores = node.get("availVirtualCores", 0)
    used_cores = node.get("usedVirtualCores", 0)
    
    total_mem = avail_mem + used_mem
    total_cores = avail_cores + used_cores
    
    mem_pct = (used_mem / total_mem * 100) if total_mem > 0 else 0.0
    core_pct = (used_cores / total_cores * 100) if total_cores > 0 else 0.0
    
    status_color = "\033[1;32m" if state == "RUNNING" else "\033[1;31m"
    
    print(f"Node ID: \033[1;34m{node_id}\033[0m")
    print(f"  Hostname:  {host}")
    print(f"  State:     {status_color}{state}\033[0m")
    print(f"  Containers Running: {containers}")
    print(f"  Memory allocation:  {used_mem} MB / {total_mem} MB ({mem_pct:.1f}% used)")
    print(f"  CPU Core allocation: {used_cores} vCores / {total_cores} vCores ({core_pct:.1f}% used)")
    print("-" * 50)
' "$RESPONSE"

echo -e "\n${YELLOW}=== YARN Cluster Capacity Resource Checking ===${NC}"
docker exec "$CONTAINER_NAME" yarn node -list -all
