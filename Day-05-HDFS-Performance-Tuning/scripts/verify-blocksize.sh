#!/bin/bash
# Script to verify the configured default HDFS block size on written files

echo "[+] Creating a test file in HDFS to verify default block size configuration..."
hdfs dfs -mkdir -p /tmp/verify_blocksize

# Create a small local file and upload it
dd if=/dev/zero of=/tmp/local_test_block.dat bs=1M count=5 status=none
hdfs dfs -put -f /tmp/local_test_block.dat /tmp/verify_blocksize/test.dat
rm -f /tmp/local_test_block.dat

echo "[+] Inspecting file block metadata..."
BLOCK_SIZE_BYTES=$(hdfs dfs -stat "%o" /tmp/verify_blocksize/test.dat)
BLOCK_SIZE_MB=$((BLOCK_SIZE_BYTES / 1024 / 1024))

echo "[+] File: /tmp/verify_blocksize/test.dat"
echo "[+] Block Size: $BLOCK_SIZE_BYTES Bytes ($BLOCK_SIZE_MB MB)"

if [ "$BLOCK_SIZE_BYTES" -eq 268435456 ]; then
    echo -e "\033[0;32m[SUCCESS]\033[0m Default HDFS block size is correctly configured to 256MB."
else
    echo -e "\033[0;33m[WARNING]\033[0m Default block size is $BLOCK_SIZE_MB MB, expected 256 MB. Ensure hdfs-site.xml is properly loaded."
fi

# Cleanup
hdfs dfs -rm -r -f /tmp/verify_blocksize
