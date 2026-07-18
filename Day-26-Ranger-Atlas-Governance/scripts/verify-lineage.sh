#!/usr/bin/env bash
# verify-lineage.sh
# Queries Apache Atlas REST API to verify table-to-table lineage graph.

set -eo pipefail

ATLAS_URL=${1:-"http://localhost:21000"}
ATLAS_USER="admin"
ATLAS_PASS="admin"
TABLE_NAME="transactions_summary"

echo "=========================================================="
echo "🔗 Verifying Metadata Lineage in Apache Atlas..."
echo "=========================================================="

# 1. Search for the Hive table entity
echo "🔍 Searching for entity '${TABLE_NAME}' in Hive database..."
SEARCH_RESPONSE=$(curl -s -u "${ATLAS_USER}:${ATLAS_PASS}" \
    -H "Content-Type: application/json" \
    "${ATLAS_URL}/api/atlas/v2/search/basic?typeName=hive_table&excludeDeletedEntities=true&limit=1&query=${TABLE_NAME}")

# Extract GUID of the table
ENTITY_GUID=$(echo "${SEARCH_RESPONSE}" | grep -o '"guid":"[^"]*"' | head -n 1 | cut -d'"' -f4 || true)

if [ -z "${ENTITY_GUID}" ]; then
    echo "❌ Table '${TABLE_NAME}' was not found in Atlas metadata repository."
    echo "Please ensure Hive has executed queries to register this table, and that the hook sent events."
    exit 1
fi

echo "✅ Table Found! GUID: ${ENTITY_GUID}"

# 2. Query Lineage for this Entity
echo "📈 Retrieving lineage graph..."
LINEAGE_RESPONSE=$(curl -s -u "${ATLAS_USER}:${ATLAS_PASS}" \
    "${ATLAS_URL}/api/atlas/v2/lineage/${ENTITY_GUID}?direction=BOTH&depth=3")

# Check if lineage response contains relations
RELATION_COUNT=$(echo "${LINEAGE_RESPONSE}" | grep -o '"relationshipId"' | wc -l || echo "0")

if [ "$RELATION_COUNT" -gt 0 ]; then
    echo "✅ Lineage Graph successfully fetched. Path contains ${RELATION_COUNT} transformations/nodes."
    echo "ℹ️ Visual Graph Data Summary:"
    echo "${LINEAGE_RESPONSE}" | grep -E '"(guid|typeName|displayText|displayText)"' | sed 's/^[ \t]*//' | sort -u || true
else
    echo "⚠️ Lineage graph is empty. Table registered but no input/output transformations (CTAS or Insert) found."
fi

echo "=========================================================="
echo "🎯 Atlas Lineage Verification Completed!"
echo "=========================================================="
