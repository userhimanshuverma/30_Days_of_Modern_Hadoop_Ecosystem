#!/usr/bin/env bash
# produce-consume-demo.sh
# End-to-end verification script for topic creation, message production, consumption, and cleanup.

set -euo pipefail

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

DEMO_TOPIC="day10-demo-topic"

echo -e "${YELLOW}=== Kafka End-to-End Ingestion Diagnostics ===${NC}"

# Check if at least one broker container is running
if ! docker ps --format '{{.Names}}' | grep -q "^kafka1-day10$"; then
  echo -e "${RED}[ERROR] Container 'kafka1-day10' is not running! Cannot execute ingestion demo.${NC}"
  exit 1
fi

# Step 1: Create Topic
echo -e "\n1. Creating high-availability topic: '${GREEN}${DEMO_TOPIC}${NC}' with 3 partitions and replication-factor 3..."
if ! docker exec kafka1-day10 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic "$DEMO_TOPIC" \
  --partitions 3 \
  --replication-factor 3 \
  --if-not-exists; then
  echo -e "${RED}[FAIL] Failed to create topic.${NC}"
  exit 1
fi

# Show the topic metadata
echo -e "\n2. Verifying topic configuration:"
docker exec kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$DEMO_TOPIC"

# Step 2: Produce Messages
echo -e "\n3. Producing 5 keyed events to '${DEMO_TOPIC}'..."
temp_events=$(mktemp)
cat <<EOF > "$temp_events"
user_id_1:{"event_type": "page_view", "timestamp": 1719827000}
user_id_2:{"event_type": "click", "timestamp": 1719827005}
user_id_1:{"event_type": "add_to_cart", "timestamp": 1719827010}
user_id_3:{"event_type": "page_view", "timestamp": 1719827015}
user_id_2:{"event_type": "purchase", "timestamp": 1719827020}
EOF

# Pipe the temp events into the container's standard input
docker exec -i kafka1-day10 kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$DEMO_TOPIC" \
  --property parse.key=true \
  --property key.separator=: < "$temp_events"

rm -f "$temp_events"
echo -e "${GREEN}[OK] Events successfully written to Kafka.${NC}"

# Step 3: Consume Messages
echo -e "\n4. Reading events back from '${DEMO_TOPIC}' (using a consumer group to verify offset tracking)..."
docker exec kafka1-day10 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$DEMO_TOPIC" \
  --from-beginning \
  --max-messages 5 \
  --property print.key=true \
  --property key.separator=" -> " \
  --group "day10-demo-group"

# Step 4: Validate Consumer Group
echo -e "\n5. Describing consumer group 'day10-demo-group' to verify partition offset tracking:"
docker exec kafka1-day10 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group "day10-demo-group"

# Step 5: Clean up Topic
echo -e "\n6. Cleaning up: Deleting topic '${DEMO_TOPIC}'..."
docker exec kafka1-day10 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic "$DEMO_TOPIC"

echo -e "\n${GREEN}[SUCCESS] End-to-end Kafka demo complete. Production and consumption succeeded!${NC}"
exit 0
