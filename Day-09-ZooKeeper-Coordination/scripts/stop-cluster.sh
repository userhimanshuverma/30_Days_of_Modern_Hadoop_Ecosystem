#!/usr/bin/env bash
# stop-cluster.sh
# Stops the ZooKeeper cluster and removes volumes.

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="${SCRIPT_DIR}/../docker"

echo -e "${YELLOW}=== Stopping Apache ZooKeeper 3-Node Ensemble ===${NC}"

cd "${DOCKER_DIR}"
docker compose down -v

echo -e "${GREEN}[SUCCESS] ZooKeeper ensemble stopped and clean-up completed.${NC}"
