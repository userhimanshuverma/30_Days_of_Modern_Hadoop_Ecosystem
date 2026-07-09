#!/bin/bash
# Day 18: Apache Spark Structured Streaming Bootstrap script
# Location: Day-18-Spark-Structured-Streaming/docker/bootstrap.sh

# Exit on error
set -e

# Setup passwordless SSH if needed
if [ -d "/var/run/sshd" ]; then
    /usr/sbin/sshd
fi

ROLE=$1
shift

echo "Starting Spark Container role: $ROLE"

# Common environment settings
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_CONF_DIR=/opt/spark/conf

# Ensure scripts are executable
if [ -d "/workspace/scripts" ]; then
  chmod +x /workspace/scripts/*.sh || true
fi

case "$ROLE" in
  namenode)
    # Format Namenode if not formatted
    if [ ! -d "/hadoop/dfs/name/current" ]; then
      echo "Formatting HDFS NameNode metadata..."
      /opt/hadoop/bin/hdfs namenode -format -force
    fi
    echo "Launching HDFS NameNode..."
    exec /opt/hadoop/bin/hdfs namenode
    ;;

  datanode)
    echo "Launching HDFS DataNode..."
    exec /opt/hadoop/bin/hdfs datanode
    ;;

  spark-master)
    echo "Launching Spark Standalone Master..."
    cp /workspace/configs/spark-defaults.conf /opt/spark/conf/
    cp /workspace/configs/spark-env.sh /opt/spark/conf/
    cp /workspace/configs/log4j2.properties /opt/spark/conf/
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
    ;;

  spark-worker)
    echo "Waiting for Spark Master UI to be online..."
    until curl -s http://spark-master:8080/ > /dev/null; do
      echo "Spark Master Web UI is offline. Retrying in 3 seconds..."
      sleep 3
    done
    echo "Launching Spark Standalone Worker..."
    cp /workspace/configs/spark-defaults.conf /opt/spark/conf/
    cp /workspace/configs/spark-env.sh /opt/spark/conf/
    cp /workspace/configs/log4j2.properties /opt/spark/conf/
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    ;;

  spark-history)
    echo "Waiting for HDFS NameNode to be online..."
    until curl -s http://namenode:9870/ > /dev/null; do
      echo "HDFS NameNode Web UI is offline. Retrying in 3 seconds..."
      sleep 3
    done

    # Create the logging directory in HDFS
    echo "Initializing HDFS directory for Spark event logs..."
    /opt/hadoop/bin/hadoop fs -mkdir -p /shared/spark-logs
    /opt/hadoop/bin/hadoop fs -chmod 777 /shared/spark-logs

    echo "Launching Spark History Server..."
    cp /workspace/configs/spark-defaults.conf /opt/spark/conf/
    cp /workspace/configs/spark-env.sh /opt/spark/conf/
    cp /workspace/configs/log4j2.properties /opt/spark/conf/
    
    # Run history server in the foreground
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer
    ;;

  spark-client)
    echo "Waiting for Spark Master to be online..."
    until curl -s http://spark-master:8080/ > /dev/null; do
      echo "Spark Master Web UI is offline. Retrying..."
      sleep 3
    done
    cp /workspace/configs/spark-defaults.conf /opt/spark/conf/
    cp /workspace/configs/spark-env.sh /opt/spark/conf/
    cp /workspace/configs/log4j2.properties /opt/spark/conf/

    echo "Spark client node is ready."
    # Keep container alive
    exec tail -f /dev/null
    ;;

  *)
    echo "Unknown container role: $ROLE. Executing raw command: $@"
    exec "$@"
    ;;
esac
