#!/usr/bin/env bash

# Day 16: Apache Spark Environment Configurations
# Location: Day-16-Spark-Core-Architecture/configs/spark-env.sh

# Set Java home and Hadoop config directories
export JAVA_HOME=/usr/local/openjdk-8
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_CONF_DIR=/opt/spark/conf

# Spark standalone master configurations
export SPARK_MASTER_HOST=spark-master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

# Spark worker configurations
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=2g
export SPARK_WORKER_WEBUI_PORT=8081

# Spark History Server UI port
export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18080 -Dspark.history.retainedApplications=50"
export SPARK_LOG_DIR=/opt/spark/logs
