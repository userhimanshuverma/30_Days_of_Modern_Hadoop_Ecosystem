#!/usr/bin/env bash
# verify-consumer.sh
# Verifies consumption of events from the 'orders' topic.

set -e

TOPIC="orders"
CONTAINER_NAME="kafka1-day11"

echo "=== Kafka Consumer Verification Script ==="

if docker ps --format '{{.Names}}' | grep -q "${CONTAINER_NAME}"; then
  echo "[✓] Docker container ${CONTAINER_NAME} is active."

  echo "--------------------------------------------------------"
  echo "Choose an option to consume events:"
  echo "1) Start a simple console consumer (view all historical events, CTRL+C to exit)"
  echo "2) Run the pre-packaged Java OrderConsumer (Manual Commit, CTRL+C to exit)"
  echo "3) Skip interactive test"
  echo "--------------------------------------------------------"
  read -p "Enter choice [1-3]: " CHOICE

  case "$CHOICE" in
    1)
      echo "Starting console consumer..."
      docker exec -it "${CONTAINER_NAME}" kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic "${TOPIC}" \
        --from-beginning \
        --property print.key=true \
        --property print.value=true \
        --property key.separator=" -> "
      ;;
    2)
      echo "Navigating to labs directory and checking Java consumer jar..."
      CURR_DIR=$(pwd)
      SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
      cd "${SCRIPT_DIR}/../labs"
      if [ -f "target/day-11-kafka-clients-1.0-SNAPSHOT.jar" ]; then
         echo "Jar exists. Executing OrderConsumer..."
      else
         echo "Compiling Maven project..."
         mvn clean package -DskipTests
      fi
      java -cp target/day-11-kafka-clients-1.0-SNAPSHOT.jar com.hadoop.kafka.OrderConsumer ../configs/consumer.properties
      cd "${CURR_DIR}"
      ;;
    *)
      echo "Skipping consumer test."
      ;;
  esac

else
  echo "[X] Error: Docker container ${CONTAINER_NAME} is not running."
  echo "Please start the local Kafka cluster first."
  exit 1
fi
echo "=== Verification complete ==="
