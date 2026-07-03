#!/usr/bin/env bash
# verify-schema-compatibility.sh
# Checks compatibility of local Avro schema changes against Schema Registry.

set -euo pipefail

REGISTRY_URL="http://localhost:8081"
SUBJECT="day-12-users-value"

echo "=== Schema Compatibility Verification Script ==="
echo "[*] Schema Registry URL: ${REGISTRY_URL}"
echo "[*] Subject to Test:     ${SUBJECT}"

# Ensure Schema Registry is running
if ! curl -s -o /dev/null -w "%{http_code}" "${REGISTRY_URL}" &>/dev/null; then
  echo "[X] Error: Schema Registry is unreachable. Make sure the cluster is running."
  exit 1
fi

# Ensure the subject exists, register v1 first if it doesn't
echo "Checking if subject '${SUBJECT}' exists..."
SUBJECT_EXISTS=$(curl -s -o /dev/null -w "%{http_code}" "${REGISTRY_URL}/subjects/${SUBJECT}/versions/latest" || true)

if [ "${SUBJECT_EXISTS}" = "404" ]; then
  echo "[!] Subject not found. Registering schemas/user-v1.avsc as version 1 first..."
  # Clean json escaping for Avro schema payload
  ESCAPED_V1=$(cat ../schemas/user-v1.avsc | jq -Rs .)
  curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "{\"schema\": ${ESCAPED_V1}}" \
    "${REGISTRY_URL}/subjects/${SUBJECT}/versions"
  echo -e "\n[✓] Registered v1 schema."
fi

# 1. Validate compatible schema (user-v2-compatible.avsc)
echo -e "\n[1] Testing compatibility of 'schemas/user-v2-compatible.avsc' against latest version..."
ESCAPED_V2_COMPAT=$(cat ../schemas/user-v2-compatible.avsc | jq -Rs .)
RESPONSE_COMPAT=$(curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": ${ESCAPED_V2_COMPAT}}" \
  "${REGISTRY_URL}/compatibility/subjects/${SUBJECT}/versions/latest")

echo "Response from Registry: ${RESPONSE_COMPAT}"
IS_COMPATIBLE=$(echo "${RESPONSE_COMPAT}" | jq -r '.is_compatible // empty')

if [ "${IS_COMPATIBLE}" = "true" ]; then
  echo "[✓] SUCCESS: user-v2-compatible.avsc is COMPATIBLE."
else
  echo "[X] FAILURE: user-v2-compatible.avsc compatibility test failed: ${RESPONSE_COMPAT}"
fi

# 2. Validate incompatible schema (user-v2-incompatible.avsc)
echo -e "\n[2] Testing compatibility of 'schemas/user-v2-incompatible.avsc' against latest version..."
ESCAPED_V2_INCOMPAT=$(cat ../schemas/user-v2-incompatible.avsc | jq -Rs .)
RESPONSE_INCOMPAT=$(curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": ${ESCAPED_V2_INCOMPAT}}" \
  "${REGISTRY_URL}/compatibility/subjects/${SUBJECT}/versions/latest")

echo "Response from Registry: ${RESPONSE_INCOMPAT}"
IS_INCOMPATIBLE=$(echo "${RESPONSE_INCOMPAT}" | jq -r '.is_compatible // empty')

if [ "${IS_INCOMPATIBLE}" = "false" ]; then
  echo "[✓] SUCCESS: Schema Registry correctly flagged user-v2-incompatible.avsc as INCOMPATIBLE!"
else
  echo "[X] FAILURE: Expected incompatible response, but registry returned: ${RESPONSE_INCOMPAT}"
fi

echo -e "\n=== Compatibility Verification Complete ==="
