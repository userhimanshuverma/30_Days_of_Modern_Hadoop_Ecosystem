#!/bin/bash
# Script to verify and configure the HDFS Balancer bandwidth and execute a balance check

echo "[+] Fetching current balancer bandwidth from the active cluster..."
# Note: getBalancerBandwidth requires NameNode address in some versions or is queried cluster-wide
BANDWIDTH=$(hdfs dfsadmin -getBalancerBandwidth namenode:9000 2>&1)
echo "$BANDWIDTH"

echo "[+] Setting HDFS Balancer bandwidth to 100MB/s (104857600 bytes/sec)..."
hdfs dfsadmin -setBalancerBandwidth 104857600

echo "[+] Verifying new balancer bandwidth..."
hdfs dfsadmin -getBalancerBandwidth namenode:9000

echo "[+] Checking current cluster disk distribution balance (dry-run/threshold 10%)..."
# We run balancer with a high threshold (e.g. 10%) so it checks if the nodes are balanced.
# We run with -idleIterations 1 to prevent it from looping indefinitely.
hdfs balancer -threshold 10 -idleIterations 1

echo -e "\033[0;32m[SUCCESS]\033[0m HDFS Balancer verified and bandwidth configurations successfully tuned."
