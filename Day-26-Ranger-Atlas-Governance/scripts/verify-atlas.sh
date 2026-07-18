#!/usr/bin/env bash
# verify-atlas.sh
# Verifies Apache Atlas service health and queries types from REST API.

set -eo pipefail

ATLAS_URL=${1:-"http://localhost:21000"}
ATLAS_USER="admin"
ATLAS_PASS="admin" # default dev credential

echo "=========================================================="
echo "🔍 Checking Apache Atlas Governance Status..."
echo "=========================================================="

# 1. Healthcheck Ping
VERSION_JSON=$(curl -s -f -u "${ATLAS_USER}:${ATLAS_PASS}" "${ATLAS_URL}/api/atlas/admin/version")
if [ $? -eq 0 ]; then
    VERSION=$(echo "${VERSION_JSON}" | grep -o '"Version":"[^"]*"' | cut -d'"' -f4 || echo "2.x")
    echo "✅ [Atlas REST API] - Connected successfully (Version: ${VERSION})."
else
    echo "❌ [Atlas REST API] - Failed to connect or retrieve version."
    echo "Ensure that Docker container 'atlas' is healthy and port 21000 is open."
    exit 1
fi

# 2. Retrieve Entity Types Count
echo "📋 Fetching registered entity types (hive, hdfs, etc.)..."
TYPEDEFS_JSON=$(curl -s -u "${ATLAS_USER}:${ATLAS_PASS}" -H "Accept: application/json" "${ATLAS_URL}/api/atlas/v2/types/typedefs")

# Simple check for some core types
for type in "hive_table" "hive_db" "hdfs_path"; do
    if echo "${TYPEDEFS_JSON}" | grep -q "\"name\":\"${type}\""; then
        echo "✅ [Type Definition] - '${type}' type is successfully registered in Atlas."
    else
        echo "⚠️ [Type Definition] - '${type}' not found. Hive Hook may need configuration."
    fi
done

echo "=========================================================="
echo "🎯 Atlas Governance Verification Completed Successfully!"
echo "=========================================================="
