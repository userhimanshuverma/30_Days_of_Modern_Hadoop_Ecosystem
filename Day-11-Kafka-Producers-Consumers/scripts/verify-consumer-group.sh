#!/usr/bin/env bash
# verify-consumer-group.sh
# Inspects active consumer groups, member partition assignments, and lag metrics.

set -e

GROUP="order-processing-group"
CONTAINER_NAME="kafka1-day11"

echo "=== Kafka Consumer Group Verification Script ==="

if docker ps --format '{{.Names}}' | grep -q "${CONTAINER_NAME}"; then
  echo "Listing all active consumer groups in the cluster:"
  echo "--------------------------------------------------------"
  docker exec -t "${CONTAINER_NAME}" kafka-consumer-groups --bootstrap-server localhost:9092 --list
  echo "--------------------------------------------------------"

  echo "Describing consumer group '${GROUP}' (Offsets, Log End Offsets, Lag, Hosts):"
  echo "--------------------------------------------------------------------------------------------------------"
  # Capture stderr to handle cases where group doesn't exist yet
  docker exec -t "${CONTAINER_NAME}" kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group "${GROUP}" || {
    echo "Note: Group '${GROUP}' may not be active yet. Run the Java Consumer first to register the group."
  }
  echo "--------------------------------------------------------------------------------------------------------"

  echo "Checking members of consumer group '${GROUP}':"
  echo "--------------------------------------------------------------------------------------------------------"
  docker exec -t "${CONTAINER_NAME}" kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group "${GROUP}" --members || true
  echo "--------------------------------------------------------------------------------------------------------"

else
  echo "[X] Error: Docker container ${CONTAINER_NAME} is not running."
  exit 1
fi
