#!/usr/bin/env bash
# verify-ranger.sh
# Verifies that Ranger Admin is up, responding, and serving policy queries.

set -eo pipefail

RANGER_URL=${1:-"http://localhost:6080"}
RANGER_USER="admin"
RANGER_PASS="RangerAdminPassword123"

echo "=========================================================="
echo "🔍 Checking Apache Ranger Admin Status..."
echo "=========================================================="

# 1. Healthcheck Ping
if curl -s -f -u "${RANGER_USER}:${RANGER_PASS}" "${RANGER_URL}/service/plugins/policies/download/production_hive" > /dev/null; then
    echo "✅ [Ranger Admin API] - Connected successfully."
else
    echo "❌ [Ranger Admin API] - Failed to connect or download policies."
    echo "Check if the Docker containers are running and port 6080 is accessible."
    exit 1
fi

# 2. Retrieve Hive Policies
echo "📋 Fetching policies for service 'production_hive'..."
POLICIES_JSON=$(curl -s -u "${RANGER_USER}:${RANGER_PASS}" -H "Accept: application/json" "${RANGER_URL}/service/public/v2/api/service/production_hive/policy")

POLICY_COUNT=$(echo "${POLICIES_JSON}" | grep -o '"id":' | wc -l || echo "0")
echo "✅ Found ${POLICY_COUNT} policies registered in 'production_hive'."

# Print policy details
if [ "$POLICY_COUNT" -gt 0 ]; then
    echo "🔍 Registered Policy List:"
    echo "${POLICIES_JSON}" | grep -E '"(id|name|resources)"' | sed 's/^[ \t]*//' || true
else
    echo "⚠️ No custom policies created yet. Default policies may apply."
fi

echo "=========================================================="
echo "🎯 Ranger Admin Verification Completed Successfully!"
echo "=========================================================="
