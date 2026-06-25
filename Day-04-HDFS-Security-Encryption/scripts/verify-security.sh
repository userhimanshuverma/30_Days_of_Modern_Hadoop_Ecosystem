#!/bin/bash
# verify-security.sh - Master runner executing all validation tests.
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "================================================================"
echo "🛡️  STARTING HDFS SECURITY VALIDATION SUITE"
echo "================================================================"

# Ensure execution flags are set
chmod +x "$SCRIPT_DIR"/verify-*.sh

# 1. Run Kerberos verification
if ! bash "$SCRIPT_DIR/verify-kerberos.sh"; then
    echo "❌ Kerberos verification failed! Aborting."
    exit 1
fi
echo ""

# 2. Run SSL/TLS verification
if ! bash "$SCRIPT_DIR/verify-tls.sh"; then
    echo "❌ SSL/TLS verification failed! Aborting."
    exit 1
fi
echo ""

# 3. Run ACL verification
if ! bash "$SCRIPT_DIR/verify-acls.sh"; then
    echo "❌ ACL verification failed! Aborting."
    exit 1
fi
echo ""

# 4. Run Encryption verification
if ! bash "$SCRIPT_DIR/verify-encryption.sh"; then
    echo "❌ HDFS Encryption verification failed! Aborting."
    exit 1
fi
echo ""

echo "================================================================"
echo "🏆 SUCCESS: ALL SECURE HADOOP COMPLIANCE CHECKS PASSED!"
echo "================================================================"
exit 0
