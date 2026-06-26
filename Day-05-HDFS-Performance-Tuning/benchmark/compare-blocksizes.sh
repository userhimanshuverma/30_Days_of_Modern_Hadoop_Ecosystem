#!/bin/bash
# Script to compare HDFS write throughput across different HDFS block sizes

TEST_FILE="/tmp/blocksize_test_512m.bin"
HDFS_DIR="/tmp/blocksize_test"

echo "[+] Generating 512MB test dataset locally in client..."
dd if=/dev/urandom of="$TEST_FILE" bs=1M count=512 status=progress

# Prepare target directory
hdfs dfs -rm -r -f "$HDFS_DIR"
hdfs dfs -mkdir -p "$HDFS_DIR"

echo "============================================="
echo " BENCHMARKING BLOCK SIZE: 32 MB"
echo "============================================="
time_32m=$( { time hdfs dfs -Ddfs.blocksize=33554432 -put -f "$TEST_FILE" "$HDFS_DIR"/test_32m.bin ; } 2>&1 | grep real | awk '{print $2}' )
echo "[+] Block Size 32MB Completed in: $time_32m"
hdfs dfs -stat "Block Size: %o | Replication: %r | Bytes: %b" "$HDFS_DIR"/test_32m.bin

echo "============================================="
echo " BENCHMARKING BLOCK SIZE: 128 MB"
echo "============================================="
time_128m=$( { time hdfs dfs -Ddfs.blocksize=134217728 -put -f "$TEST_FILE" "$HDFS_DIR"/test_128m.bin ; } 2>&1 | grep real | awk '{print $2}' )
echo "[+] Block Size 128MB Completed in: $time_128m"
hdfs dfs -stat "Block Size: %o | Replication: %r | Bytes: %b" "$HDFS_DIR"/test_128m.bin

echo "============================================="
echo " BENCHMARKING BLOCK SIZE: 256 MB"
echo "============================================="
time_256m=$( { time hdfs dfs -Ddfs.blocksize=268435456 -put -f "$TEST_FILE" "$HDFS_DIR"/test_256m.bin ; } 2>&1 | grep real | awk '{print $2}' )
echo "[+] Block Size 256MB Completed in: $time_256m"
hdfs dfs -stat "Block Size: %o | Replication: %r | Bytes: %b" "$HDFS_DIR"/test_256m.bin

# Clean up
echo "[+] Cleaning up test data..."
rm -f "$TEST_FILE"
hdfs dfs -rm -r -f "$HDFS_DIR"

echo "============================================="
echo " SUMMARY"
echo "============================================="
echo "Block size 32MB write time:  $time_32m"
echo "Block size 128MB write time: $time_128m"
echo "Block size 256MB write time: $time_256m"
echo "============================================="
echo "A larger block size minimizes block allocation metadata requests to the NameNode, resulting in faster and more contiguous write streaming."
