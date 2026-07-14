#!/bin/bash
# Day 23: Apache Trino Federated Queries Lab — HDFS & Hive Bootstrap script
# Location: Day-23-Trino-MPP-Engine/docker/bootstrap.sh

# Exit on error
set -e

ROLE=$1
shift

echo "Starting Hive Cluster container role: $ROLE"

# Common environment settings
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export HIVE_CONF_DIR=/opt/hive/conf

# Ensure verification/lab scripts are executable
if [ -d "/workspace/scripts" ]; then
  chmod +x /workspace/scripts/*.sh || true
fi

# Copy configurations if they exist in the mounted workspace
copy_configs() {
  if [ -d "/workspace/configs" ]; then
    echo "Copying cluster configurations to component config directories..."
    cp -f /workspace/configs/core-site.xml /opt/hadoop/etc/hadoop/
    cp -f /workspace/configs/hdfs-site.xml /opt/hadoop/etc/hadoop/
    cp -f /workspace/configs/core-site.xml /opt/hive/conf/
    cp -f /workspace/configs/hive-site.xml /opt/hive/conf/
  fi
}

case "$ROLE" in
  namenode)
    copy_configs
    # Format Namenode if not formatted
    if [ ! -d "/hadoop/dfs/name/current" ]; then
      echo "Formatting HDFS NameNode metadata..."
      /opt/hadoop/bin/hdfs namenode -format -force
    fi
    echo "Launching HDFS NameNode..."
    exec /opt/hadoop/bin/hdfs namenode
    ;;

  datanode)
    copy_configs
    echo "Launching HDFS DataNode..."
    exec /opt/hadoop/bin/hdfs datanode
    ;;

  metastore)
    copy_configs

    echo "Waiting for HDFS NameNode (RPC 9000) to be online..."
    until nc -z namenode-day23 9000; do
      echo "HDFS NameNode is offline. Retrying in 2 seconds..."
      sleep 2
    done

    # Ensure HDFS system directories are created for Hive
    echo "Creating HDFS system directories for Hive (/tmp and /user/hive/warehouse)..."
    /opt/hadoop/bin/hadoop fs -mkdir -p /tmp
    /opt/hadoop/bin/hadoop fs -mkdir -p /user/hive/warehouse
    /opt/hadoop/bin/hadoop fs -chmod g+w /tmp
    /opt/hadoop/bin/hadoop fs -chmod g+w /user/hive/warehouse

    echo "Waiting for PostgreSQL Metastore database to be online..."
    until pg_isready -h postgres-metastore-db-day23 -U hive -d metastore_db; do
      echo "PostgreSQL DB is offline. Retrying in 2 seconds..."
      sleep 2
    done

    # Initialize Schema if not already initialized
    echo "Checking if Hive Metastore Schema is initialized..."
    if /opt/hive/bin/schematool -dbType postgres -info > /dev/null 2>&1; then
      echo "Hive Metastore schema is already initialized."
    else
      echo "Hive Metastore schema not initialized. Initializing now..."
      /opt/hive/bin/schematool -dbType postgres -initSchema
    fi

    echo "Launching Hive Metastore Server (HMS)..."
    exec /opt/hive/bin/hive --service metastore
    ;;

  hiveserver2)
    copy_configs

    echo "Waiting for Hive Metastore (HMS Thrift 9083) to be online..."
    until nc -z hive-metastore-day23 9083; do
      echo "Hive Metastore Service is offline. Retrying in 2 seconds..."
      sleep 2
    done

    echo "Launching HiveServer2 (HS2)..."
    exec /opt/hive/bin/hive --service hiveserver2
    ;;

  *)
    echo "Unknown container role: $ROLE. Executing raw command: $@"
    exec "$@"
    ;;
esac
