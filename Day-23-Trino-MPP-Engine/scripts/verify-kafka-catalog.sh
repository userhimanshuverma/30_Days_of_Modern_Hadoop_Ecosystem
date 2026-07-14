#!/bin/bash
# Day 23: Trino Validation Script - Verify Kafka Catalog
# Location: Day-23-Trino-MPP-Engine/scripts/verify-kafka-catalog.sh

echo "========================================================="
echo "Verifying Kafka Topic to Trino Catalog Mapping"
echo "========================================================="

# 1. Create the topic in Kafka
echo "Creating Kafka topic 'clicks'..."
docker exec -t kafka-day23 kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic clicks || true

# 2. Produce mock JSON records to Kafka
echo "Producing mock JSON clicks data..."
docker exec -i kafka-day23 kafka-console-producer --bootstrap-server localhost:9092 --topic clicks <<EOF
{"click_id":"c-101","user_id":1,"page_url":"/home","click_timestamp":"2026-07-14T12:00:00Z"}
{"click_id":"c-102","user_id":2,"page_url":"/products","click_timestamp":"2026-07-14T12:05:00Z"}
{"click_id":"c-103","user_id":1,"page_url":"/checkout","click_timestamp":"2026-07-14T12:10:00Z"}
EOF

# 3. Give Kafka a moment to sync metadata
sleep 3

# 4. Read data through Trino
echo "Querying kafka.default.clicks via Trino..."
docker exec -t trino-coordinator-day23 trino --execute "SELECT _message, click_id, user_id, page_url, click_timestamp FROM kafka.default.clicks"

if [ $? -eq 0 ]; then
  echo "✔ Kafka catalog integration validated successfully."
else
  echo "✘ Failed to query Kafka topic through Trino. Check kafka schema json mapping or connector settings."
  exit 1
fi
