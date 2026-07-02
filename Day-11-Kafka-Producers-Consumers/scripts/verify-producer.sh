#!/usr/bin/env bash
# verify-producer.sh
# Checks if Kafka Docker cluster is running, creates the 'orders' topic, and tests producing messages.

set -e

# Base variables
TOPIC="orders"
BOOTSTRAP_SERVER="localhost:19092"
CONTAINER_NAME="kafka1-day11"

echo "=== Kafka Producer Verification Script ==="

# Check if Docker container is running
if docker ps --format '{{.Names}}' | grep -q "${CONTAINER_NAME}"; then
  echo "[✓] Docker container ${CONTAINER_NAME} is active."
  
  # Check if topic exists; if not, create it
  echo "Checking if topic '${TOPIC}' exists..."
  TOPIC_EXISTS=$(docker exec -t "${CONTAINER_NAME}" kafka-topics --bootstrap-server localhost:9092 --list | grep -w "${TOPIC}" || true)
  
  if [ -z "${TOPIC_EXISTS}" ]; then
    echo "Topic '${TOPIC}' does not exist. Creating it with 3 partitions and a replication factor of 3..."
    docker exec -t "${CONTAINER_NAME}" kafka-topics --bootstrap-server localhost:9092 \
      --create --topic "${TOPIC}" --partitions 3 --replication-factor 3 \
      --config min.insync.replicas=2
    echo "[✓] Topic '${TOPIC}' successfully created."
  else
    echo "[✓] Topic '${TOPIC}' already exists."
    # Describe topic to ensure configuration matches requirements
    docker exec -t "${CONTAINER_NAME}" kafka-topics --bootstrap-server localhost:9092 --describe --topic "${TOPIC}"
  fi

  # Prompt user to select testing method
  echo "--------------------------------------------------------"
  echo "You can now test producing events. Choose an option:"
  echo "1) Start a manual interactive console producer (CTRL+D to exit)"
  echo "2) Run the pre-packaged Java OrderProducer (100 events)"
  echo "3) Skip interactive test"
  echo "--------------------------------------------------------"
  read -p "Enter choice [1-3]: " CHOICE

  case "$CHOICE" in
    1)
      echo "Starting console producer. Type messages and press Enter. Format: Key:Value (e.g. cust_1:{\"orderId\":\"123\",\"amount\":99.9})"
      docker exec -it "${CONTAINER_NAME}" kafka-console-producer \
        --bootstrap-server localhost:9092 \
        --topic "${TOPIC}" \
        --property parse.key=true \
        --property key.separator=:
      ;;
    2)
      echo "Navigating to labs directory and compiling Java code..."
      # Save current working directory
      CURR_DIR=$(pwd)
      SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
      cd "${SCRIPT_DIR}/../labs"
      if [ -f "target/day-11-kafka-clients-1.0-SNAPSHOT.jar" ]; then
         echo "Jar already compiled. Executing OrderProducer..."
      else
         echo "Compiling Maven project..."
         mvn clean package -DskipTests
      fi
      java -cp target/day-11-kafka-clients-1.0-SNAPSHOT.jar com.hadoop.kafka.OrderProducer ../configs/producer.properties
      cd "${CURR_DIR}"
      ;;
    *)
      echo "Skipping production test."
      ;;
  esac

else
  echo "[X] Error: Docker container ${CONTAINER_NAME} is not running."
  echo "Please start the local Kafka cluster first: docker-compose -f ../docker/docker-compose.yml up -d"
  exit 1
fi
echo "=== Verification complete ==="
