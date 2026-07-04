#!/usr/bin/env bash
# verify-kafka-topic.sh — Day 13 Ingestion Pipeline Verification
# Verifies Kafka topic metadata, partitions, and replication status.

set -euo pipefail

TOPIC_NAME=${1:-clickstream-events}
CONTAINER_NAME="kafka-day13"

echo "=== [Verification: Kafka Topic Metadata] ==="
echo "[*] Target Topic: $TOPIC_NAME"

# 1. Check if docker is running
if ! command -v docker &>/dev/null; then
    echo "[X] Error: 'docker' CLI is not found."
    exit 1
fi

# 2. Check if the Kafka container is running
if ! docker ps --filter "name=$CONTAINER_NAME" --format '{{.Names}}' | grep -q "$CONTAINER_NAME"; then
    echo "[X] Error: Kafka container '$CONTAINER_NAME' is not running."
    echo "    Start the infrastructure first by running docker-compose up."
    exit 1
fi
echo "[✓] Kafka container '$CONTAINER_NAME' is running."

# 3. Query topic configuration using kafka-topics.sh in container
echo "[*] Describing topic partitions and replication setup..."
if ! docker exec "$CONTAINER_NAME" kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$TOPIC_NAME" &>/tmp/topic_desc; then
    echo "[X] Error: Topic '$TOPIC_NAME' does not exist or broker is unresponsive."
    exit 1
fi

cat /tmp/topic_desc
rm -f /tmp/topic_desc

# 4. Show a sample message count from each partition
echo -e "\n[*] Inspecting message log offsets per partition..."
docker exec "$CONTAINER_NAME" kafka-run-class.sh kafka.tools.GetOffsetShell \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC_NAME" \
    --time -1

echo "[✓] Topic verification complete."
exit 0
