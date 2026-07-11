#!/bin/bash
# Day 20: Apache Hive & Metastore Master Verification Script
# Location: Day-20-Hive-Architecture-HMS/scripts/verify-hive.sh

set -e

# Work from the scripts directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "========================================================================="
echo "🐝 Starting Apache Hive & Metastore (HMS) Integration Tests"
echo "========================================================================="

# 1. Run Metastore verification
echo ""
echo "🔄 [Test 1/3] Verifying Hive Metastore Service (HMS)..."
"$SCRIPT_DIR/verify-metastore.sh" hive-metastore

# 2. Run HiveServer2 verification
echo ""
echo "🔄 [Test 2/3] Verifying HiveServer2 (HS2) JDBC Connectivity..."
"$SCRIPT_DIR/verify-hiveserver2.sh" localhost

# 3. Run Table Operations verification
echo ""
echo "🔄 [Test 3/3] Verifying Table Operations (DDL, DML, Managed/External)..."
"$SCRIPT_DIR/verify-tables.sh" localhost

echo ""
echo "========================================================================="
echo "🎉 SUCCESS: All Apache Hive verification stages completed successfully!"
echo "========================================================================="
