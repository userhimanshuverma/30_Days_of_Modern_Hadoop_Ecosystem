#!/bin/bash
# Day 22 — Hive Storage Formats & ACID Transactions Bootstrap Script
# Location: Day-22-Hive-Formats-ACID/docker/bootstrap.sh

# Exit on error
set -e

ROLE=$1
shift

echo "Starting Hive Cluster container role: $ROLE"

# Common environment settings
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export HIVE_CONF_DIR=/opt/hive/conf
export TEZ_CONF_DIR=/opt/tez/conf
export SPARK_CONF_DIR=/opt/spark/conf
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$TEZ_CONF_DIR:/opt/tez/*:/opt/tez/lib/*

# Copy configuration files if they exist in the mounted workspace
copy_configs() {
  if [ -d "/workspace/configs" ]; then
    echo "Copying cluster configurations to component config directories..."
    cp -r /workspace/configs/core-site.xml /opt/hadoop/etc/hadoop/ 2>/dev/null || true
    cp -r /workspace/configs/hdfs-site.xml /opt/hadoop/etc/hadoop/ 2>/dev/null || true
    cp -r /workspace/configs/yarn-site.xml /opt/hadoop/etc/hadoop/ 2>/dev/null || true
    cp -r /workspace/configs/mapred-site.xml /opt/hadoop/etc/hadoop/ 2>/dev/null || true
    
    cp -r /workspace/configs/hive-site.xml /opt/hive/conf/ 2>/dev/null || true
    cp -r /workspace/configs/tez-site.xml /opt/hive/conf/ 2>/dev/null || true
    cp -r /workspace/configs/tez-site.xml /opt/tez/conf/ 2>/dev/null || true
    
    mkdir -p /opt/spark/conf
    cp -r /workspace/configs/spark-defaults.conf /opt/spark/conf/ 2>/dev/null || true
    cp -r /workspace/configs/hive-site.xml /opt/spark/conf/ 2>/dev/null || true
  fi
}

# Make scripts in workspace executable
if [ -d "/workspace/scripts" ]; then
  chmod +x /workspace/scripts/*.sh || true
fi

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

  resourcemanager)
    copy_configs
    echo "Launching YARN ResourceManager..."
    exec /opt/hadoop/bin/yarn resourcemanager
    ;;

  nodemanager)
    copy_configs
    echo "Launching YARN NodeManager..."
    exec /opt/hadoop/bin/yarn nodemanager
    ;;

  metastore)
    copy_configs

    echo "Waiting for HDFS NameNode (RPC 9000) to be online..."
    until nc -z namenode 9000; do
      echo "HDFS NameNode is offline. Retrying in 2 seconds..."
      sleep 2
    done

    # Ensure HDFS system directories are created for Hive
    echo "Creating HDFS system directories for Hive..."
    /opt/hadoop/bin/hadoop fs -mkdir -p /tmp
    /opt/hadoop/bin/hadoop fs -mkdir -p /user/hive/warehouse
    /opt/hadoop/bin/hadoop fs -chmod g+w /tmp
    /opt/hadoop/bin/hadoop fs -chmod g+w /user/hive/warehouse

    # Upload Tez Tarball to HDFS for task container execution
    echo "Checking Tez Tarball on HDFS..."
    /opt/hadoop/bin/hadoop fs -mkdir -p /apps/tez
    if ! /opt/hadoop/bin/hadoop fs -test -e /apps/tez/tez-0.10.2.tar.gz; then
      echo "Uploading Tez Tarball to HDFS (/apps/tez/tez-0.10.2.tar.gz)..."
      /opt/hadoop/bin/hadoop fs -put /opt/tez/tez-0.10.2.tar.gz /apps/tez/
    fi

    echo "Waiting for PostgreSQL Metastore database to be online..."
    until pg_isready -h postgres-metastore-db -U hive -d metastore_db; do
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
    until nc -z hive-metastore 9083; do
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
