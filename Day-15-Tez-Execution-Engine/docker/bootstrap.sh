#!/bin/bash

# Exit on error
set -e

# Setup passwordless SSH if needed
if [ -d "/var/run/sshd" ]; then
    /usr/sbin/sshd
fi

# Define component role based on container command or argument
ROLE=$1
shift

echo "Starting container role: $ROLE"

# Common environment settings
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export TEZ_CONF_DIR=/opt/tez/conf
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$TEZ_CONF_DIR:/opt/tez/*:/opt/tez/lib/*

case "$ROLE" in
  namenode)
    # Format Namenode if not formatted
    if [ ! -d "/hadoop/dfs/name/current" ]; then
      echo "Formatting HDFS NameNode metadata..."
      /opt/hadoop/bin/hdfs namenode -format -force
    fi

    # Start NameNode in foreground
    echo "Launching HDFS NameNode..."
    exec /opt/hadoop/bin/hdfs namenode
    ;;

  datanode)
    echo "Launching HDFS DataNode..."
    exec /opt/hadoop/bin/hdfs datanode
    ;;

  resourcemanager)
    echo "Launching YARN ResourceManager..."
    exec /opt/hadoop/bin/yarn resourcemanager
    ;;

  nodemanager)
    echo "Launching YARN NodeManager..."
    exec /opt/hadoop/bin/yarn nodemanager
    ;;

  hiveserver2)
    echo "Waiting for HDFS NameNode and DataNode to be healthy..."
    until curl -s http://namenode:9870/ > /dev/null; do
      echo "NameNode Web UI is offline. Retrying in 3 seconds..."
      sleep 3
    done
    echo "HDFS is online. Checking YARN ResourceManager..."
    until curl -s http://resourcemanager:8088/ > /dev/null; do
      echo "ResourceManager Web UI is offline. Retrying in 3 seconds..."
      sleep 3
    done

    # Setup directories on HDFS
    echo "Initializing HDFS directory layouts..."
    /opt/hadoop/bin/hadoop fs -mkdir -p /tmp
    /opt/hadoop/bin/hadoop fs -mkdir -p /user/hive/warehouse
    /opt/hadoop/bin/hadoop fs -chmod g+w /tmp
    /opt/hadoop/bin/hadoop fs -chmod g+w /user/hive/warehouse

    # Make workspace scripts executable
    if [ -d "/workspace/scripts" ]; then
      echo "Making scripts in /workspace/scripts executable..."
      chmod +x /workspace/scripts/*.sh || true
    fi

    # Upload Tez Tarball to HDFS if not exists
    echo "Checking Tez Libraries on HDFS..."
    /opt/hadoop/bin/hadoop fs -mkdir -p /apps/tez
    if ! /opt/hadoop/bin/hadoop fs -test -e /apps/tez/tez-0.10.2.tar.gz; then
      echo "Uploading Tez Tarball (/opt/tez/tez-0.10.2.tar.gz) to HDFS..."
      /opt/hadoop/bin/hadoop fs -put /opt/tez/tez-0.10.2.tar.gz /apps/tez/
    fi

    # Initialize Hive Metastore database schema (Derby)
    echo "Initializing Hive Metastore Schema..."
    mkdir -p /var/lib/hive
    cd /var/lib/hive
    if [ ! -d "/var/lib/hive/metastore_db" ]; then
      echo "Creating fresh Derby Metastore DB schema..."
      /opt/hive/bin/schematool -dbType derby -initSchema
    else
      echo "Derby Metastore already initialized."
    fi

    # Start HiveServer2
    echo "Launching Apache HiveServer2 with Apache Tez execution engine..."
    exec /opt/hive/bin/hiveserver2
    ;;

  *)
    echo "Unknown container role: $ROLE. Executing raw command: $@"
    exec "$@"
    ;;
esac
