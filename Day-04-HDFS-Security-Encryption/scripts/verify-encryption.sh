#!/bin/bash
# verify-encryption.sh - Verifies HDFS Transparent Data Encryption (TDE) with KMS.
set -eo pipefail

echo "============================================="
echo "🔒 Verifying Transparent Data Encryption (TDE)..."
echo "============================================="

KEYTAB_DIR="/etc/security/keytabs"

# 1. Login as hdfs superuser
echo "Logging in as admin 'hdfs' to manage encryption keys..."
kinit -kt "$KEYTAB_DIR/hdfs.keytab" hdfs@HADOOP.LOCAL

# 2. Check and Create KMS Key
echo "Checking KMS keys..."
if hadoop key list | grep -q "finance-key"; then
    echo "✔ [OK] Key 'finance-key' already exists in KMS."
else
    echo "Creating new encryption key 'finance-key' in KMS..."
    hadoop key create finance-key
    echo "✔ [OK] Key 'finance-key' created successfully."
fi

# 3. Create HDFS path and make it an Encryption Zone (EZ)
echo "Setting up Encryption Zone at '/finance-zone'..."
hdfs dfs -rm -r -f /finance-zone || true
hdfs dfs -mkdir -p /finance-zone

echo "Provisioning Encryption Zone..."
hdfs crypto -createZone -keyName finance-key -path /finance-zone
echo "✔ [OK] Encryption Zone established on '/finance-zone'."

# 4. Grant write ACL to Alice so she can test writing to the EZ
hdfs dfs -setfacl -m user:alice:rwx /finance-zone

# 5. Log in as client user 'alice'
echo "Switching to client user 'alice' to write encrypted data..."
kinit -kt "$KEYTAB_DIR/alice.keytab" alice@HADOOP.LOCAL

# 6. Write secret content to the zone
SECRET_CONTENT="CONFIDENTIAL-BANK-PAYROLL-DATA-2026-DO-NOT-SHARE"
echo "Writing sensitive text file to /finance-zone/payroll.txt..."
echo "$SECRET_CONTENT" | hdfs dfs -put - /finance-zone/payroll.txt

# 7. Read decrypted content
echo "Reading file back from HDFS (Decryption on-the-fly):"
DECRYPTED_CONTENT=$(hdfs dfs -cat /finance-zone/payroll.txt)
echo "Content: $DECRYPTED_CONTENT"

if [ "$DECRYPTED_CONTENT" = "$SECRET_CONTENT" ]; then
    echo "✔ [OK] Read & Decrypt process verified!"
else
    echo "❌ [ERROR] Decrypted content mismatch!"
    exit 1
fi

# 8. Query metadata to identify physical Block ID
echo "Locating physical block metadata via fsck..."
BLOCK_INFO=$(hdfs fsck /finance-zone/payroll.txt -files -blocks 2>/dev/null || true)
BLOCK_ID=$(echo "$BLOCK_INFO" | grep -o "blk_[0-9]*" | head -n 1 || true)

if [ -n "$BLOCK_ID" ]; then
    echo "✔ [OK] File mapped to block: $BLOCK_ID"
    echo "--------------------------------------------------------------------------------"
    echo "💡 PROOF OF ENCRYPTION AT REST:"
    echo "If you log into the 'datanode' container and search for this block file:"
    echo "  docker exec -it datanode find /var/lib/hadoop/dfs/data/current/ -name ${BLOCK_ID}"
    echo "and read it, you will see encrypted ciphertext (not our plaintext string)."
    echo "--------------------------------------------------------------------------------"
else
    echo "⚠️ [WARN] Could not retrieve Block ID from fsck output."
fi

# Clean up
kinit -kt "$KEYTAB_DIR/hdfs.keytab" hdfs@HADOOP.LOCAL >/dev/null
hdfs dfs -rm -r -f /finance-zone
kinit -kt "$KEYTAB_DIR/alice.keytab" alice@HADOOP.LOCAL >/dev/null

echo "✔ [SUCCESS] HDFS TDE verification tests passed!"
