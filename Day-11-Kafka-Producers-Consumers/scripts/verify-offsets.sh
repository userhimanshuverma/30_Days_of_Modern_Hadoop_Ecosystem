#!/usr/bin/env bash
# verify-offsets.sh
# Queries log-end-offsets and log-start-offsets for the 'orders' topic.

set -e

TOPIC="orders"
CONTAINER_NAME="kafka1-day11"

echo "=== Kafka Offsets Verification Script ==="

if docker ps --format '{{.Names}}' | grep -q "${CONTAINER_NAME}"; then
  echo "Fetching offsets for topic '${TOPIC}'..."
  echo "--------------------------------------------------------"
  echo "Partition | Log Start Offset | Log End Offset | Message Count"
  echo "--------------------------------------------------------"
  
  # Run GetOffsetShell for earliest and latest offsets and parse
  # Format returned: topic:partition:offset
  EARLIEST=$(docker exec -t "${CONTAINER_NAME}" kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server localhost:9092 --topic "${TOPIC}" --time -2)
    
  LATEST=$(docker exec -t "${CONTAINER_NAME}" kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server localhost:9092 --topic "${TOPIC}" --time -1)

  # Parse results
  IFS=$'\n'
  for line in $LATEST; do
    # Remove carriage return if any
    line=$(echo "$line" | tr -d '\r')
    if [ -n "$line" ]; then
      part=$(echo "$line" | cut -d':' -f2)
      lat_off=$(echo "$line" | cut -d':' -f3)
      
      # Find matching earliest offset
      ear_off=$(echo "$EARLIEST" | tr -d '\r' | grep -w "${TOPIC}:${part}" | cut -d':' -f3)
      
      # Calculate count
      count=$((lat_off - ear_off))
      
      printf "    %-5s |      %-11s |     %-10s | %-10s\n" "$part" "$ear_off" "$lat_off" "$count"
    fi
  done
  echo "--------------------------------------------------------"

else
  echo "[X] Error: Docker container ${CONTAINER_NAME} is not running."
  exit 1
fi
