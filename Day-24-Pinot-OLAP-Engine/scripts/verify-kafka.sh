#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIGS_DIR="$ROOT_DIR/configs"

echo "======================================================================"
echo "📝 Verifying Kafka Connection and Initializing Topics"
echo "======================================================================"

# Check if Kafka container is running
if ! docker ps | grep -q pinot-kafka; then
    echo "❌ Error: pinot-kafka container is not running."
    echo "Please start the cluster using: docker compose -f $ROOT_DIR/docker/docker-compose.yml up -d"
    exit 1
fi

echo "✅ Kafka container is running."

# Wait for Kafka to be ready
echo "Waiting for Kafka broker to accept connections..."
until docker exec pinot-kafka kafka-topics --bootstrap-server kafka:9092 --list > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo ""
echo "✅ Kafka broker is ready."

# Check if the topic already exists
echo "Checking if topic 'user-registrations' exists..."
TOPICS=$(docker exec pinot-kafka kafka-topics --bootstrap-server kafka:9092 --list)

if echo "$TOPICS" | grep -q "user-registrations"; then
    echo "⚠️ Topic 'user-registrations' already exists."
else
    echo "Creating Kafka topic 'user-registrations' with 2 partitions..."
    docker exec pinot-kafka kafka-topics --bootstrap-server kafka:9092 --create --topic user-registrations --partitions 2 --replication-factor 1
    echo "✅ Topic 'user-registrations' created successfully."
fi

# Produce sample data to Kafka
echo "Producing sample events to topic 'user-registrations'..."
if [ -f "$CONFIGS_DIR/sample-events.json" ]; then
    docker exec -i pinot-kafka kafka-console-producer --bootstrap-server kafka:9092 --topic user-registrations < "$CONFIGS_DIR/sample-events.json"
    echo "✅ Sample events successfully pushed to Kafka."
else
    echo "❌ Error: sample-events.json not found at $CONFIGS_DIR/sample-events.json"
    exit 1
fi

echo "======================================================================"
echo "🎉 Kafka Initialization Completed Successfully!"
echo "======================================================================"
