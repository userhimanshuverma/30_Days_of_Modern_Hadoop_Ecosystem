#!/usr/bin/env bash
# verify-partitions.sh
# Diagnostic script to analyze topic partition layouts, leaders, and offset ranges.

set -euo pipefail

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== Kafka Partition Layout & Offset Range Diagnostics ===${NC}"

# Check if at least one broker container is running
if ! docker ps --format '{{.Names}}' | grep -q "^kafka1-day10$"; then
  echo -e "${RED}[ERROR] Container 'kafka1-day10' is not running! Cannot query partitions.${NC}"
  exit 1
fi

target_topic="${1:-}"

if [ -z "$target_topic" ]; then
  # Grab first custom topic that is not internal (e.g. not __consumer_offsets)
  target_topic=$(docker exec kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -v "^__" | head -n 1 || true)
  if [ -z "$target_topic" ]; then
    echo -e "${YELLOW}[NOTE] No user-created topics found. Please specify a topic name or create one first.${NC}"
    exit 0
  fi
  echo -e "No topic specified. Defaulting to first custom topic found: '${GREEN}${target_topic}${NC}'\n"
fi

echo -e "Analyzing partition leadership and distribution for topic '${target_topic}':"
docker exec kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$target_topic"

echo -e "\nQuerying partition offset ranges (Log End Offsets) for topic '${target_topic}':"
# Get offsets using GetOffsetShell
docker exec kafka1-day10 kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic "$target_topic"

# Let's count leadership per broker to check for distribution balance
echo -e "\nBroker partition leadership counts for '${target_topic}':"
docker exec kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$target_topic" | \
  grep -o 'Leader: [0-9]\+' | sort | uniq -c | \
  while read -r count leader_label; do
    echo -e " - Broker ${leader_label#Leader: }: leading ${GREEN}${count}${NC} partitions"
  done

exit 0
