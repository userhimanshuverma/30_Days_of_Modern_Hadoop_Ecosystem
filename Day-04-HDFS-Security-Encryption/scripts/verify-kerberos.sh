#!/bin/bash
# verify-kerberos.sh - Verifies Kerberos credentials, principal logins, and ticket caches.
set -eo pipefail

echo "============================================="
echo "🔒 Verifying Kerberos KDC & Keytabs..."
echo "============================================="

KEYTAB_DIR="/etc/security/keytabs"

# 1. Check Keytab existence
KEYTABS=("nn" "dn" "kms" "spnego" "alice" "hdfs")
for kt in "${KEYTABS[@]}"; do
    if [ ! -f "$KEYTAB_DIR/$kt.keytab" ]; then
        echo "❌ [ERROR] Keytab $KEYTAB_DIR/$kt.keytab not found!"
        exit 1
    fi
    echo "✔ [OK] Keytab found: $kt.keytab"
done

# 2. Test Client Login (alice)
echo -n "Testing authentication for user principal 'alice@HADOOP.LOCAL'... "
kinit -kt "$KEYTAB_DIR/alice.keytab" alice@HADOOP.LOCAL
if [ $? -eq 0 ]; then
    echo "✔ SUCCESS!"
else
    echo "❌ FAILED!"
    exit 1
fi

# Print active ticket details
echo "--- Active Ticket Cache ---"
klist
echo "---------------------------"

# 3. Test Administrative Login (hdfs)
echo -n "Testing authentication for admin principal 'hdfs@HADOOP.LOCAL'... "
kinit -kt "$KEYTAB_DIR/hdfs.keytab" hdfs@HADOOP.LOCAL
if [ $? -eq 0 ]; then
    echo "✔ SUCCESS!"
else
    echo "❌ FAILED!"
    exit 1
fi

# Clean up ticket cache and re-auth as alice for downstream testing
kdestroy
kinit -kt "$KEYTAB_DIR/alice.keytab" alice@HADOOP.LOCAL >/dev/null

echo "✔ [SUCCESS] Kerberos authentication tests passed!"
