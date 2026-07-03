#!/usr/bin/env bash
# verify-schema-version.sh
# Tests registering schemas, updating versions, and querying registered schemas by ID.

set -euo pipefail

REGISTRY_URL="http://localhost:8081"
SUBJECT="day-12-users-value"

echo "=== Schema Versioning & Registration Script ==="
echo "[*] Target URL: ${REGISTRY_URL}"
echo "[*] Subject:    ${SUBJECT}"

# Ensure Schema Registry is running
if ! curl -s -o /dev/null -w "%{http_code}" "${REGISTRY_URL}" &>/dev/null; then
  echo "[X] Error: Schema Registry is unreachable. Please launch the docker cluster first."
  exit 1
fi

# 1. Register user-v1.avsc
echo -e "\n[1] Registering schemas/user-v1.avsc..."
ESCAPED_V1=$(cat ../schemas/user-v1.avsc | jq -Rs .)
V1_REG_RESP=$(curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": ${ESCAPED_V1}}" \
  "${REGISTRY_URL}/subjects/${SUBJECT}/versions")
echo "Registry Response: ${V1_REG_RESP}"
V1_SCHEMA_ID=$(echo "${V1_REG_RESP}" | jq -r '.id // empty')
echo "[✓] Registered v1 with Schema ID: ${V1_SCHEMA_ID}"

# 2. Register user-v2-compatible.avsc (Evolve to version 2)
echo -e "\n[2] Registering schemas/user-v2-compatible.avsc (Evolved schema)..."
ESCAPED_V2=$(cat ../schemas/user-v2-compatible.avsc | jq -Rs .)
V2_REG_RESP=$(curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": ${ESCAPED_V2}}" \
  "${REGISTRY_URL}/subjects/${SUBJECT}/versions")
echo "Registry Response: ${V2_REG_RESP}"
V2_SCHEMA_ID=$(echo "${V2_REG_RESP}" | jq -r '.id // empty')
echo "[✓] Registered v2 with Schema ID: ${V2_SCHEMA_ID}"

# 3. Retrieve all versions
echo -e "\n[3] Querying all versions for subject '${SUBJECT}'..."
VERSIONS=$(curl -s "${REGISTRY_URL}/subjects/${SUBJECT}/versions")
echo "Registered Versions: ${VERSIONS}"

# 4. Fetch schema by specific version (e.g. Version 1)
echo -e "\n[4] Querying schema metadata for Version 1..."
V1_METADATA=$(curl -s "${REGISTRY_URL}/subjects/${SUBJECT}/versions/1")
echo "Version 1 Detail: $(echo "${V1_METADATA}" | jq -c '.')"

# 5. Fetch schema by Schema ID
echo -e "\n[5] Querying raw schema definition by global Schema ID: ${V1_SCHEMA_ID}..."
RAW_SCHEMA=$(curl -s "${REGISTRY_URL}/schemas/ids/${V1_SCHEMA_ID}")
echo "Schema for ID ${V1_SCHEMA_ID}: $(echo "${RAW_SCHEMA}" | jq -c '.schema')"

echo -e "\n=== Version Validation Complete ==="
