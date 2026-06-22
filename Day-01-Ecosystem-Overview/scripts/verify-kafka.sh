#!/usr/bin/env bash
# =========================================================================
# Kafka Ingestion Stream Validation Script - Day 1
# Tests topic creation, message production, and message consumption.
# =========================================================================

set -euo pipefail

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

KAFKA_CONTAINER="kafka-day01"
ZK_CONTAINER="zookeeper-day01"
TOPIC_NAME="day01-validation-topic"
TEST_MESSAGE="Kafka Day 1: Real-time Event Streaming Successful!"

echo -e "${YELLOW}=== Starting Kafka Ingestion Validation ===${NC}"

# 1. Check if Containers are Running
if ! docker ps --filter "name=${ZK_CONTAINER}" --filter "status=running" | grep -q "${ZK_CONTAINER}"; then
  echo -e "${RED}[ERROR] ZooKeeper container '${ZK_CONTAINER}' is not running.${NC}"
  exit 1
fi
if ! docker ps --filter "name=${KAFKA_CONTAINER}" --filter "status=running" | grep -q "${KAFKA_CONTAINER}"; then
  echo -e "${RED}[ERROR] Kafka container '${KAFKA_CONTAINER}' is not running.${NC}"
  exit 1
fi
echo -e "${GREEN}[OK] ZooKeeper and Kafka containers are running.${NC}"

# 2. Wait for Kafka Broker to be online
echo "Waiting for Kafka broker to be responsive..."
for i in {1..30}; do
  if docker exec "${KAFKA_CONTAINER}" kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
    echo -e "${GREEN}[OK] Kafka Broker is responding.${NC}"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo -e "${RED}[ERROR] Kafka failed to respond after 30 seconds.${NC}"
    exit 1
  fi
  sleep 2
done

# 3. Create Kafka Topic
echo "Creating Kafka topic '${TOPIC_NAME}'..."
docker exec "${KAFKA_CONTAINER}" kafka-topics \
  --create \
  --topic "${TOPIC_NAME}" \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1

echo -e "${GREEN}[OK] Topic created.${NC}"

# 4. Produce a Message
echo "Publishing test event to topic..."
echo "${TEST_MESSAGE}" | docker exec -i "${KAFKA_CONTAINER}" kafka-console-producer \
  --topic "${TOPIC_NAME}" \
  --bootstrap-server kafka:9092

echo -e "${GREEN}[OK] Test event published.${NC}"

# 5. Consume the Message
echo "Consuming test event from topic..."
CONSUMED_MESSAGE=$(docker exec "${KAFKA_CONTAINER}" kafka-console-consumer \
  --topic "${TOPIC_NAME}" \
  --bootstrap-server kafka:9092 \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 5000 2>/dev/null || true)

# 6. Verify Content
if [[ "$CONSUMED_MESSAGE" == *"${TEST_MESSAGE}"* ]]; then
  echo -e "${GREEN}[OK] Consume test successful. Retained identical event payload.${NC}"
else
  echo -e "${RED}[ERROR] Consume test failed. Got: ${CONSUMED_MESSAGE}${NC}"
  # Clean up before exiting
  docker exec "${KAFKA_CONTAINER}" kafka-topics --delete --topic "${TOPIC_NAME}" --bootstrap-server kafka:9092 >/dev/null 2>&1 || true
  exit 1
fi

# 7. Cleanup
echo "Deleting Kafka test topic..."
docker exec "${KAFKA_CONTAINER}" kafka-topics \
  --delete \
  --topic "${TOPIC_NAME}" \
  --bootstrap-server kafka:9092

echo -e "${GREEN}=== Kafka Ingestion Validation PASSED successfully! ===${NC}"
