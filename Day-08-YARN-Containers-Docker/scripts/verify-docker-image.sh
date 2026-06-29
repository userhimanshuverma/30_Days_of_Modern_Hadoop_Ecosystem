#!/usr/bin/env bash
# ==============================================================================
# verify-docker-image.sh
# Verifies that required Docker images are built, loaded, or pullable.
# ==============================================================================

set -eo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

IMAGE_NAME="yarn-sample-app:latest"

echo "=========================================================="
echo "⚡ DIAGNOSTICS: Verifying Docker Image Availability"
echo "=========================================================="

echo "Targeting Image: $IMAGE_NAME"

# 1. Check local images
if docker image inspect "$IMAGE_NAME" &> /dev/null; then
    echo -e "${GREEN}[OK] Image '$IMAGE_NAME' found locally.${NC}"
    image_size=$(docker image inspect "$IMAGE_NAME" --format '{{.Size}}')
    echo "Image Size: $((image_size / 1024 / 1024)) MB"
else
    echo -e "${RED}[WARNING] Image '$IMAGE_NAME' not found locally. YARN will need to pull it or it must be built using scripts/run-docker-demo.sh.${NC}"
fi

# 2. Check registry connectivity (optional check)
echo -e "${GREEN}[INFO] Testing connection to public Docker registry (hub.docker.com)...${NC}"
if curl -sI https://registry-1.docker.io/v2/ &> /dev/null; then
    echo -e "${GREEN}[OK] Connection to public Docker registry is successful.${NC}"
else
    echo -e "${RED}[WARNING] Public Docker registry not reachable. Ensure offline caching is working if using local-only images.${NC}"
fi

echo "=========================================================="
echo -e "${GREEN}Docker image verification completed!${NC}"
echo "=========================================================="
