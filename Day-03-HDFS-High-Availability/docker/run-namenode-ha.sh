#!/usr/bin/env bash
# run-namenode-ha.sh
# Custom entrypoint wrapper for HDFS NameNodes in High Availability mode.
# Orchestrates cluster formatting, JournalNode synchronization, ZKFC formatting, and Standby bootstrapping.

set -euo pipefail

# Ensure configurations are pointing to our mounted config files
export HADOOP_CONF_DIR="/etc/hadoop"

echo "=== Starting HDFS HA NameNode Bootstrapping & Process Monitor ==="
echo "Node ID: ${NODE_ID}"

# Helper function to wait for a TCP port to open
wait_for_port() {
  local host=$1
  local port=$2
  local description=$3
  echo "Waiting for ${description} (${host}:${port}) to be available..."
  while ! timeout 1 bash -c "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null; do
    sleep 2
  done
  echo "[OK] ${description} is online."
}

# 1. Wait for critical coordinate services to be online
wait_for_port "zookeeper" 2181 "ZooKeeper Coordination Quorum"
wait_for_port "journalnode1" 8485 "JournalNode 1"
wait_for_port "journalnode2" 8485 "JournalNode 2"
wait_for_port "journalnode3" 8485 "JournalNode 3"

# Create required directories if they don't exist
mkdir -p /hadoop/dfs/name

# 2. Node-specific HA Cluster Setup
if [ "${NODE_ID}" = "nn1" ]; then
  # =========================================================================
  # PRIMARY NAMENODE CONFIGURATION (nn1)
  # =========================================================================
  if [ ! -f /hadoop/dfs/name/current/VERSION ]; then
    echo "Primary NameNode (nn1) is not formatted. Initiating format sequence..."
    
    # Format the NameNode filesystem
    hdfs namenode -format -clusterId mycluster -nonInteractive
    
    # Initialize the JournalNodes shared directory
    echo "Initializing Shared Edits in JournalNodes..."
    hdfs namenode -initializeSharedEdits -force -nonInteractive
    
    # Format ZKFC metadata state in ZooKeeper
    echo "Formatting ZooKeeper Failover Controller (ZKFC) path..."
    hdfs zkfc -formatZK -nonInteractive
  else
    echo "Primary NameNode (nn1) is already formatted. Skipping format sequence."
  fi

elif [ "${NODE_ID}" = "nn2" ]; then
  # =========================================================================
  # STANDBY NAMENODE CONFIGURATION (nn2)
  # =========================================================================
  # Wait for Primary NameNode RPC port to be active
  wait_for_port "namenode1" 9000 "Primary NameNode RPC"

  if [ ! -f /hadoop/dfs/name/current/VERSION ]; then
    echo "Standby NameNode (nn2) is not bootstrapped. Copying state from Primary (nn1)..."
    # Bootstrap standby node state from the active node
    hdfs namenode -bootstrapStandby -nonInteractive
  else
    echo "Standby NameNode (nn2) is already bootstrapped. Skipping bootstrap."
  fi

else
  echo "[FATAL] Unknown NODE_ID: ${NODE_ID}. Must be 'nn1' or 'nn2'."
  exit 1
fi

# 3. Start ZooKeeper Failover Controller (ZKFC) daemon in background
echo "Starting ZooKeeper Failover Controller (ZKFC) on local host..."
hdfs zkfc &
ZKFC_PID=$!

# Handle shutdown signals gracefully
cleanup() {
  echo "Received termination signal. Stopping services..."
  kill -TERM "$ZKFC_PID" 2>/dev/null || true
  exit 0
}
trap cleanup SIGTERM SIGINT

# 4. Start the NameNode daemon in foreground (replaces shell execution)
echo "Starting HDFS NameNode daemon..."
exec hdfs namenode
