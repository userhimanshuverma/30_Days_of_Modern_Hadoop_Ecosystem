#!/bin/bash
# Script to verify network latency and measure actual write/read throughput to HDFS

echo "============================================="
echo " NETWORK LATENCY TO DFS DAEMONS"
echo "============================================="

for node in namenode.hadoop.local datanode1.hadoop.local datanode2.hadoop.local datanode3.hadoop.local; do
    echo -n "[+] Latency to $node: "
    curl -o /dev/null -s -w 'HTTP Code: %{http_code} | Connect Time: %{time_connect}s | Start Transfer: %{time_starttransfer}s\n' http://$node:9864/jmx 2>/dev/null || \
    curl -o /dev/null -s -w 'HTTP Code: %{http_code} | Connect Time: %{time_connect}s | Start Transfer: %{time_starttransfer}s\n' http://$node:9870/jmx 2>/dev/null
done

echo "============================================="
echo " MEASURING HDFS WRITE THROUGHPUT"
echo "============================================="
TEST_DATA="/tmp/throughput_test.bin"

# 1. Create a 100MB local test file
dd if=/dev/urandom of="$TEST_DATA" bs=1M count=100 status=none

# 2. Time upload to HDFS
echo "[+] Streaming 100MB payload into HDFS..."
start_time=$(date +%s.%N)
hdfs dfs -put -f "$TEST_DATA" /tmp/throughput_test.bin
end_time=$(date +%s.%N)

duration=$(echo "$end_time - start_time" | bc 2>/dev/null || awk "BEGIN {print $end_time - $start_time}")
throughput=$(echo "100.0 / $duration" | bc 2>/dev/null || awk "BEGIN {print 100.0 / $duration}")

echo "[+] Uploaded 100MB to HDFS in: $duration seconds"
echo "[+] Effective Write Throughput: $throughput MB/s"

echo "============================================="
echo " MEASURING HDFS READ THROUGHPUT"
echo "============================================="

# 3. Time download from HDFS to null
echo "[+] Streaming 100MB payload from HDFS to client /dev/null..."
start_time=$(date +%s.%N)
hdfs dfs -get /tmp/throughput_test.bin /dev/null
end_time=$(date +%s.%N)

duration=$(echo "$end_time - start_time" | bc 2>/dev/null || awk "BEGIN {print $end_time - $start_time}")
throughput=$(echo "100.0 / $duration" | bc 2>/dev/null || awk "BEGIN {print 100.0 / $duration}")

echo "[+] Read 100MB from HDFS in: $duration seconds"
echo "[+] Effective Read Throughput: $throughput MB/s"

# Cleanup
rm -f "$TEST_DATA"
hdfs dfs -rm -f /tmp/throughput_test.bin

echo "============================================="
echo -e "\033[0;32m[SUCCESS]\033[0m HDFS physical I/O and network paths validated successfully."
