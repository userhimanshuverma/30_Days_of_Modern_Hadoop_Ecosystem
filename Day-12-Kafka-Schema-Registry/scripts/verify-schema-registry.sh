#!/usr/bin/env bash
# verify-schema-registry.sh
# Verifies Schema Registry API health and queries active subjects and configuration levels.

set -euo pipefail

REGISTRY_URL="http://localhost:8081"
echo "=== Schema Registry API Health Check ==="
echo "[*] Target URL: ${REGISTRY_URL}"

# Check if Schema Registry port is listening
if ! curl -s -o /dev/null -w "%{http_code}" "${REGISTRY_URL}" &>/dev/null; then
  echo "[X] Error: Schema Registry is unreachable at ${REGISTRY_URL}."
  echo "    Make sure the docker container is up: docker-compose -f ../docker/docker-compose.yml up -d"
  exit 1
fi

echo "[✓] Schema Registry is online and responding."

# 1. Fetch default global compatibility config
echo "--- Fetching Global Compatibility Mode ---"
COMPATIBILITY=$(curl -s "${REGISTRY_URL}/config")
echo "Result: ${COMPATIBILITY}"

# 2. Fetch list of registered subjects
echo "--- Fetching Registered Subjects ---"
SUBJECTS=$(curl -s "${REGISTRY_URL}/subjects")
echo "Result: ${SUBJECTS}"

# 3. Test listing schemas
echo "--- Fetching Global Schema Registry Info ---"
# Schema Registry lists schemas at /schemas
SCHEMAS=$(curl -s "${REGISTRY_URL}/schemas")
echo "Result: ${SCHEMAS}"

echo "=== Verification Complete ==="
