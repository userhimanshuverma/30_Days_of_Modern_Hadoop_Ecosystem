#!/bin/bash
# Day 18: Kafka Verification and Ingestion Script
# Location: Day-18-Spark-Structured-Streaming/scripts/verify-kafka.sh

set -e

BOOTSTRAP_SERVER="kafka:29092"
TOPIC="clickstream"

echo "Checking Kafka broker connectivity at $BOOTSTRAP_SERVER..."

# 1. Check if Kafka is reachable
if ! kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --list > /dev/null 2>&1; then
  echo "Error: Cannot connect to Kafka broker at $BOOTSTRAP_SERVER"
  exit 1
fi
echo "[✓] Kafka broker is online."

# 2. Check if the topic exists; if not, create it
if kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --list | grep -q "^$TOPIC$"; then
  echo "Topic '$TOPIC' already exists."
else
  echo "Creating Kafka topic '$TOPIC'..."
  kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER \
    --create --topic $TOPIC \
    --partitions 3 \
    --replication-factor 1
  echo "[✓] Topic '$TOPIC' created successfully."
fi

# 3. Produce sample clickstream JSON data
echo "Producing sample clickstream messages..."

# Get current ISO timestamp and slightly older ones to test event time
NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
T_MINUS_1=$(date -u -d "1 minute ago" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -v-1M +"%Y-%m-%dT%H:%M:%SZ")
T_MINUS_2=$(date -u -d "2 minutes ago" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -v-2M +"%Y-%m-%dT%H:%M:%SZ")
T_MINUS_15=$(date -u -d "15 minutes ago" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -v-15M +"%Y-%m-%dT%H:%M:%SZ") # older than 10m watermark

cat <<EOF | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP_SERVER --topic $TOPIC
{"event_time": "$T_MINUS_2", "user_id": "usr-101", "action": "click", "page": "homepage"}
{"event_time": "$T_MINUS_1", "user_id": "usr-102", "action": "view", "page": "product_page"}
{"event_time": "$NOW", "user_id": "usr-101", "action": "click", "page": "cart"}
{"event_time": "$T_MINUS_15", "user_id": "usr-103", "action": "click", "page": "homepage"}
{"event_time": "$NOW", "user_id": "usr-104", "action": "purchase", "page": "checkout"}
EOF

echo "[✓] Pushed 5 test clickstream records to topic '$TOPIC'."
echo "To monitor the topic live, run: kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP_SERVER --topic $TOPIC --from-beginning"
