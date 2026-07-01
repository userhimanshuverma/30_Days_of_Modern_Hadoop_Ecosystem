#!/usr/bin/env bash
# verify-topics.sh
# Diagnostic script to list and describe topics in the Kafka cluster.

set -euo pipefail

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== Kafka Topic State Diagnostics ===${NC}"

# Check if at least one broker container is running
if ! docker ps --format '{{.Names}}' | grep -q "^kafka1-day10$"; then
  echo -e "${RED}[ERROR] Container 'kafka1-day10' is not running! Cannot query topics.${NC}"
  exit 1
fi

target_topic="${1:-}"

if [ -n "$target_topic" ]; then
  echo -e "Describing topic: ${GREEN}${target_topic}${NC}\n"
  if ! docker exec kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$target_topic"; then
    echo -e "${RED}[FAIL] Failed to describe topic '${target_topic}'. Check if it exists.${NC}"
    exit 2
  fi
else
  echo -e "Listing all topics in cluster..."
  topic_list=$(docker exec kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --list)
  
  if [ -z "$topic_list" ]; then
    echo -e "${YELLOW}[NOTE] No topics found in the cluster. Create a topic using:${NC}"
    echo "  docker exec kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --create --topic <name> --partitions 3 --replication-factor 3"
  else
    echo -e "\n${GREEN}Available Topics:${NC}"
    echo "$topic_list" | sed 's/^/ - /'
    
    echo -e "\nDescribing all topics in detail..."
    docker exec kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --describe
  fi
fi

exit 0
