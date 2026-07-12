#!/bin/bash
# Day 21 — Hive Execution Mode Validation: Apache Spark (HoS)
# Location: Day-21-Hive-Execution-Modes/scripts/verify-spark.sh

set -e

HS2_HOST=${1:-"localhost"}
HS2_PORT=10000

echo "========================================================================="
echo "🔍 Starting Hive Apache Spark Execution Mode Verification"
echo "========================================================================="

# Submit query using Beeline and force Spark execution
echo "🚀 Submitting query with hive.execution.engine=spark..."
/opt/hive/bin/beeline -u "jdbc:hive2://$HS2_HOST:$HS2_PORT" -n hive -p hivepassword -e "
CREATE DATABASE IF NOT EXISTS verify_modes_db;
USE verify_modes_db;

CREATE TABLE IF NOT EXISTS test_spark_table (id INT, value STRING) STORED AS ORC;

INSERT OVERWRITE TABLE test_spark_table VALUES 
(1, 'Hive Spark Execution 1'),
(2, 'Hive Spark Execution 2'),
(3, 'Hive Spark Execution 3'),
(4, 'Hive Spark Execution 4');

-- Force Spark execution engine
SET hive.execution.engine=spark;
SET hive.cbo.enable=true;

-- Run aggregation to compile into a Spark RDD execution plan
SELECT id, count(*) as count, max(value) as max_val 
FROM test_spark_table 
GROUP BY id;
"

echo "========================================================================="
echo "✅ Success: Query completed using Apache Spark engine!"
echo "========================================================================="
