#!/usr/bin/env bash
# verify-rebalancing.sh
# Monitors consumer group rebalancing state transitions in real time.

set -e

GROUP="order-processing-group"
CONTAINER_NAME="kafka1-day11"

echo "=== Kafka Consumer Group Rebalance Monitor ==="
echo "This script watches the state and membership changes of group '${GROUP}'."
echo "To trigger a rebalance: "
echo "  1. Start one Java Consumer in Terminal A: 'verify-consumer.sh' (choice 2)"
echo "  2. Start a second Java Consumer in Terminal B: 'verify-consumer.sh' (choice 2)"
echo "  3. Terminate one consumer (CTRL+C) and watch partitions reallocate."
echo "--------------------------------------------------------"

if docker ps --format '{{.Names}}' | grep -q "${CONTAINER_NAME}"; then
  
  echo "Starting state monitor loop (Press CTRL+C to stop)..."
  echo "--------------------------------------------------------"
  printf "%-25s | %-12s | %-12s | %-12s\n" "Timestamp" "Group State" "Members Count" "Coordinator"
  echo "--------------------------------------------------------"
  
  while true; do
    STATE_INFO=$(docker exec -t "${CONTAINER_NAME}" kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group "${GROUP}" --state 2>/dev/null || true)
    
    if [ -n "$STATE_INFO" ]; then
      # Extract state details
      STATE=$(echo "$STATE_INFO" | grep "GROUP" | awk '{print $4}' | tr -d '\r' || echo "UNKNOWN")
      MEMBERS=$(echo "$STATE_INFO" | grep "GROUP" | awk '{print $7}' | tr -d '\r' || echo "0")
      COORDINATOR=$(echo "$STATE_INFO" | grep "GROUP" | awk '{print $2}' | tr -d '\r' || echo "N/A")
      TS=$(date +"%Y-%m-%d %H:%M:%S")
      
      printf "%-25s | %-12s | %-12s | %-12s\n" "$TS" "$STATE" "$MEMBERS" "$COORDINATOR"
    else
      TS=$(date +"%Y-%m-%d %H:%M:%S")
      printf "%-25s | %-12s | %-12s | %-12s\n" "$TS" "INACTIVE/DEAD" "0" "N/A"
    fi
    sleep 2
  done

else
  echo "[X] Error: Docker container ${CONTAINER_NAME} is not running."
  exit 1
fi
