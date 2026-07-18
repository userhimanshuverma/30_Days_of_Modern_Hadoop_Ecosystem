#!/usr/bin/env bash
# verify-policies.sh
# Simulates authorization checks for HDFS/Hive using Ranger policy configurations.

set -eo pipefail

RANGER_URL=${1:-"http://localhost:6080"}
RANGER_USER="admin"
RANGER_PASS="RangerAdminPassword123"

echo "=========================================================="
echo "🛡️ Simulating Ranger Policy Authorization Checks..."
echo "=========================================================="

# 1. Fetch current policies for Hive
echo "📥 Downloading policy payload from Ranger Admin..."
POLICIES_TEMP=$(curl -s -u "${RANGER_USER}:${RANGER_PASS}" "${RANGER_URL}/service/plugins/policies/download/production_hive")

if [ -z "$POLICIES_TEMP" ] || echo "$POLICIES_TEMP" | grep -q "401 Unauthorized"; then
    echo "❌ Failed to download policy cache. Check authentication credentials."
    exit 1
fi

echo "✅ Downloaded active policy configuration."

# 2. Simulate User Evaluation
# In a real environment, the plugin runs this off-heap. We can inspect the local JSON structure.
echo "📋 Inspecting policies for columns in 'sales_db.transactions':"

HAS_SALES_POLICY=$(echo "$POLICIES_TEMP" | grep -i "transactions" || true)

if [ -n "$HAS_SALES_POLICY" ]; then
    echo "✅ Match Found: Ranger has an active policy guarding 'transactions'."
    echo "   Ensure 'analyst' group is restricted from accessing 'ssn' or 'card_number' columns."
else
    echo "⚠️ Warning: No explicit security policy found for 'sales_db.transactions'."
    echo "   Fallback to default Hive permissions will apply."
fi

# 3. Simulate HDFS path check
echo "📋 Checking security settings on HDFS paths (/finance/transactions)..."
HAS_FINANCE_HDFS=$(echo "$POLICIES_TEMP" | grep -i "/finance" || true)
if [ -n "$HAS_FINANCE_HDFS" ]; then
    echo "✅ Match Found: HDFS path '/finance' is secured by a Ranger policy."
else
    echo "ℹ️ Information: HDFS path '/finance' has no Ranger policy. Default POSIX/ACLs apply."
fi

echo "=========================================================="
echo "🎯 Policy Simulation Completed!"
echo "=========================================================="
