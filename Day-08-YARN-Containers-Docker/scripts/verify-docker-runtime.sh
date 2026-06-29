#!/usr/bin/env bash
# ==============================================================================
# verify-docker-runtime.sh
# Verifies host-level Docker installation, permissions, and configuration.
# ==============================================================================

set -eo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo "=========================================================="
echo "⚡ DIAGNOSTICS: Verifying Docker Runtime for YARN Integration"
echo "=========================================================="

# 1. Check if Docker CLI is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}[ERROR] Docker CLI is not installed or not in PATH.${NC}"
    exit 1
else
    echo -e "${GREEN}[OK] Docker CLI detected:$(docker --version)${NC}"
fi

# 2. Check if Docker Daemon is running
if ! docker info &> /dev/null; then
    echo -e "${RED}[ERROR] Cannot connect to Docker daemon. Is it running? Check permissions.${NC}"
    exit 1
else
    echo -e "${GREEN}[OK] Docker daemon is running and accessible.${NC}"
fi

# 3. Check Docker Socket permissions
SOCKET_PATH="/var/run/docker.sock"
if [ -e "$SOCKET_PATH" ]; then
    perms=$(stat -c "%a" "$SOCKET_PATH")
    owner=$(stat -c "%U:%G" "$SOCKET_PATH")
    echo -e "${GREEN}[OK] Docker socket located at $SOCKET_PATH (Perms: $perms, Owner: $owner)${NC}"
else
    echo -e "${RED}[WARNING] Docker socket not found at $SOCKET_PATH. Sibling-container setup might fail.${NC}"
fi

# 4. Check if Docker is in cgroup v2 or v1 mode
cgroup_version=$(docker info --format '{{.CgroupVersion}}' 2>/dev/null || echo "Unknown")
echo -e "${GREEN}[INFO] Host CGroup mode: v$cgroup_version${NC}"

echo "=========================================================="
echo -e "${GREEN}Docker runtime verification completed successfully!${NC}"
echo "=========================================================="
