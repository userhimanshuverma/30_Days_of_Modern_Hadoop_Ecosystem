#!/bin/bash
# Day 16: Spark Core Run and Benchmark Script
# Location: Day-16-Spark-Core-Architecture/scripts/run-spark-demo.sh

set -e

echo "========================================================="
echo "⚙️  STEP 1: Compiling Java Spark App using Maven"
echo "========================================================="
cd /workspace/source
mvn clean package

echo "========================================================="
echo "📂 STEP 2: Creating Test Data in HDFS"
echo "========================================================="
# Wait for HDFS NameNode
until curl -s http://namenode:9870/ > /dev/null; do
  echo "HDFS NameNode Web UI is offline. Waiting..."
  sleep 3
done

# Create mock data file
mkdir -p /tmp
cat <<EOF > /tmp/spark-words.txt
Apache Spark Core Architecture represents a major leap in distributed data processing.
Unlike MapReduce, Spark supports in-memory processing and lazy evaluation.
By maintaining a Directed Acyclic Graph (DAG), Spark can optimize the physical plan.
Narrow transformations do not require shuffling, but wide transformations do trigger shuffling.
This Spark Core demo utilizes custom partitioning with a HashPartitioner.
EOF

# Upload file to HDFS
/opt/hadoop/bin/hadoop fs -mkdir -p /input
/opt/hadoop/bin/hadoop fs -put -f /tmp/spark-words.txt /input/
echo "Test data uploaded to HDFS."

echo "========================================================="
echo "🚀 STEP 3: Submitting Spark Job to Standalone Cluster"
echo "========================================================="
# Clean old output directories
/opt/hadoop/bin/hadoop fs -rm -r -f /output-spark || true

# Submit Spark application using spark-submit
/opt/spark/bin/spark-submit \
  --class com.hadoop.spark.SparkWordCount \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  /workspace/source/target/spark-demo-app-1.0-SNAPSHOT.jar \
  hdfs://namenode:9000/input/spark-words.txt \
  hdfs://namenode:9000/output-spark

echo "========================================================="
echo "📊 STEP 4: Verifying Partitioned Spark Results"
echo "========================================================="
echo "Verifying output parts created in HDFS:"
/opt/hadoop/bin/hadoop fs -ls hdfs://namenode:9000/output-spark

echo "Aggregation Output Sample:"
/opt/hadoop/bin/hadoop fs -cat "hdfs://namenode:9000/output-spark/part-*" | head -n 30
echo "========================================================="
echo "🎉 Spark WordCount Execution Succeeded!"
echo "========================================================="
