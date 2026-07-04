#!/usr/bin/env bash
# run-end-to-end-demo.sh — Day 13 Ingestion Pipeline Demo
# Orchestrates the setup, production, consumption, validation, and cleanup.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"

echo "========================================================================="
echo "🚀 Starting Day 13: End-to-End Data Ingestion Pipeline Demo"
echo "========================================================================="

# 1. Start Docker Containers
echo -e "\n[*] Step 1: Starting Docker Infrastructure..."
docker compose -f "$PARENT_DIR/docker/docker-compose.yml" up -d

# 2. Wait for health checks
echo -e "\n[*] Step 2: Waiting for Kafka and MinIO to be healthy..."
while true; do
    KAFKA_STATUS=$(docker inspect --format='{{.State.Health.Status}}' kafka-day13 2>/dev/null || echo "unstarted")
    MINIO_STATUS=$(docker inspect --format='{{.State.Health.Status}}' minio-day13 2>/dev/null || echo "unstarted")
    
    echo "    -> Kafka status: $KAFKA_STATUS, MinIO status: $MINIO_STATUS"
    
    if [ "$KAFKA_STATUS" = "healthy" ] && [ "$MINIO_STATUS" = "healthy" ]; then
        echo "[✓] Services are healthy and ready!"
        break
    fi
    sleep 3
done

# 3. Install Python dependencies if missing
echo -e "\n[*] Step 3: Checking Python dependencies..."
python3 -m pip install -r "$PARENT_DIR/producer/requirements.txt" --quiet || {
    echo "[!] Warning: pip install failed. Attempting user-space installation..."
    python3 -m pip install --user -r "$PARENT_DIR/producer/requirements.txt" --quiet
}
echo "[✓] Python dependencies verified."

# 4. Create Kafka Topic Clickstream Events
echo -e "\n[*] Step 4: Creating Kafka Topic 'clickstream-events'..."
docker exec kafka-day13 kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --topic clickstream-events \
    --partitions 3 \
    --replication-factor 1 \
    --config min.insync.replicas=1 \
    --if-not-exists

# 5. Run Clickstream Event Producer
echo -e "\n[*] Step 5: Emitting clickstream logs to Kafka topic..."
python3 "$PARENT_DIR/producer/producer.py" --topic clickstream-events --count 2500 --delay 0.001

# 6. Run Consumer in background to write to MinIO
echo -e "\n[*] Step 6: Starting Consumer & Storage Writer in the background..."
python3 "$PARENT_DIR/storage/consumer_storage_writer.py" --topic clickstream-events &
CONSUMER_PID=$!
echo "[✓] Storage Writer started with PID: $CONSUMER_PID"

# 7. Wait for ingestion buffer flush
echo -e "\n[*] Step 7: Waiting 15 seconds to allow message buffer flushing..."
sleep 15

# 8. Stop the consumer gracefully via SIGINT (Ctrl+C equivalent)
echo -e "\n[*] Step 8: Stopping consumer gracefully..."
kill -2 "$CONSUMER_PID"
wait "$CONSUMER_PID" || true
echo "[✓] Consumer shutdown completed cleanly."

# 9. Verify Kafka topic contents
echo -e "\n[*] Step 9: Running verification scripts..."
bash "$SCRIPT_DIR/verify-kafka-topic.sh" clickstream-events

# 10. Verify storage objects in MinIO
echo ""
bash "$SCRIPT_DIR/verify-storage.sh"

# 11. Run detailed data validation
echo ""
bash "$SCRIPT_DIR/verify-ingestion.sh"

echo "========================================================================="
echo "🎉 Ingestion Pipeline Demo Complete and Validated!"
echo "   To clean up Docker resources, run:"
echo "   docker compose -f $PARENT_DIR/docker/docker-compose.yml down -v"
echo "========================================================================="
exit 0
