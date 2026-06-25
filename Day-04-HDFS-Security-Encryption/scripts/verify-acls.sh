#!/bin/bash
# verify-acls.sh - Verifies HDFS standard POSIX permission and extended ACL enforcement.
set -eo pipefail

echo "============================================="
echo "🔒 Verifying HDFS Permissions & ACLs..."
echo "============================================="

KEYTAB_DIR="/etc/security/keytabs"

# 1. Login as hdfs superuser
echo "Logging in as admin 'hdfs' to configure ACLs..."
kinit -kt "$KEYTAB_DIR/hdfs.keytab" hdfs@HADOOP.LOCAL

# 2. Create test directory with restricted permissions (700)
echo "Creating test directory '/acl-test-dir' and setting chmod 700..."
hdfs dfs -rm -r -f /acl-test-dir || true
hdfs dfs -mkdir -p /acl-test-dir
hdfs dfs -chmod 700 /acl-test-dir

# 3. Apply Extended ACL granting read-execute to alice
echo "Applying extended ACL: grant 'alice' read/execute permissions (r-x)..."
hdfs dfs -setfacl -m user:alice:r-x /acl-test-dir

# 4. Verify ACLs list correctly
echo "Retrieving ACL configuration for '/acl-test-dir':"
hdfs dfs -getfacl /acl-test-dir

if ! hdfs dfs -getfacl /acl-test-dir | grep -q "user:alice:r-x"; then
    echo "❌ [ERROR] ACL rule for 'alice' was not applied correctly!"
    exit 1
fi
echo "✔ [OK] Extended ACL rule verified on NameNode."

# 5. Switch to client user alice
echo "Switching credentials to user 'alice'..."
kinit -kt "$KEYTAB_DIR/alice.keytab" alice@HADOOP.LOCAL

# 6. Verify alice can read the directory (POSIX permissions would block, but ACL permits)
echo "Verifying user 'alice' can read directory content..."
if hdfs dfs -ls /acl-test-dir >/dev/null 2>&1; then
    echo "✔ [OK] Access permitted via extended ACL!"
else
    echo "❌ [ERROR] Alice was unable to access /acl-test-dir!"
    exit 1
fi

# 7. Verify alice cannot write to the directory (no w in ACL)
echo "Verifying user 'alice' cannot write files inside directory..."
if hdfs dfs -touch /acl-test-dir/testfile.txt >/dev/null 2>&1; then
    echo "❌ [ERROR] Alice successfully created a file! ACL write-prevention failed."
    exit 1
else
    echo "✔ [OK] Write access denied for 'alice' as expected."
fi

# Clean up
kinit -kt "$KEYTAB_DIR/hdfs.keytab" hdfs@HADOOP.LOCAL >/dev/null
hdfs dfs -rm -r -f /acl-test-dir
kinit -kt "$KEYTAB_DIR/alice.keytab" alice@HADOOP.LOCAL >/dev/null

echo "✔ [SUCCESS] HDFS ACL verification tests passed!"
