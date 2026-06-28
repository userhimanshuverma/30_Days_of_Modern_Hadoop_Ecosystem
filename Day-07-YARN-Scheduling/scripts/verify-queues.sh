#!/usr/bin/env bash
# verify-queues.sh
# Fetches and prints a tree representation of the active YARN Scheduler queues.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

CONTAINER_NAME="resourcemanager-day07"

echo -e "${YELLOW}=== YARN Queue Hierarchy and Allocation Analyzer ===${NC}"

# 1. Verify container status
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo -e "${RED}[ERROR] Container '${CONTAINER_NAME}' is not running.${NC}"
  exit 1
fi

# 2. Verify curl is installed locally
if ! command -v curl >/dev/null 2>&1; then
  echo -e "${RED}[ERROR] 'curl' is required on the host system to query YARN REST APIs.${NC}"
  exit 1
fi

# 3. Pull scheduling info
SCHEDULER_URL="http://localhost:8088/ws/v1/cluster/scheduler"
RESPONSE=$(curl -s -f --connect-timeout 5 "$SCHEDULER_URL" || echo "")

if [ -z "$RESPONSE" ]; then
  echo -e "${RED}[ERROR] Could not fetch scheduler info. RM might be initializing. Please wait and retry.${NC}"
  exit 1
fi

# 4. Use python to parse and print a structured tree representation
echo -e "Active YARN Scheduler Queues (REST Data Analyzer):\n"

python3 -c '
import sys, json

try:
    data = json.loads(sys.argv[1])
except Exception as e:
    print("Error parsing JSON response:", e)
    sys.exit(1)

def print_queue(q, depth=0):
    indent = "    " * depth
    name = q.get("queueName", "unknown")
    capacity = q.get("capacity", 0.0)
    max_capacity = q.get("maxCapacity", 0.0)
    used_capacity = q.get("usedCapacity", 0.0)
    state = q.get("state", "RUNNING")
    
    # Print self
    print(f"{indent}├── Queue: \033[1;32m{name:<15}\033[0m | Configured Cap: {capacity:>5.1f}% | Max: {max_capacity:>5.1f}% | Used: {used_capacity:>5.1f}% | State: {state}")
    
    # Print children if any
    child_queues = q.get("queues", {})
    if child_queues and "queue" in child_queues:
        for cq in child_queues["queue"]:
            print_queue(cq, depth + 1)

# Check scheduler type and locate queues
scheduler_info = data.get("scheduler", {}).get("schedulerInfo", {})
sched_type = scheduler_info.get("type", "unknown")
print(f"Scheduler Type: \033[1;33m{sched_type}\033[0m")

if "queues" in scheduler_info:
    # Capacity Scheduler root
    for root_q in scheduler_info["queues"]["queue"]:
        print_queue(root_q)
elif "rootQueue" in scheduler_info:
    # Fair Scheduler root or different version
    print_queue(scheduler_info["rootQueue"])
else:
    # Fallback to general listing
    print("No child queues found in primary layout. Listing root parameters:")
    print("Capacity:", scheduler_info.get("capacity", "N/A"))
    print("Used Capacity:", scheduler_info.get("usedCapacity", "N/A"))
' "$RESPONSE"

echo -e "\n${YELLOW}=== YARN CLI Queue Verification ===${NC}"
echo "Running: yarn queue -status default"
docker exec "$CONTAINER_NAME" yarn queue -status default || true
