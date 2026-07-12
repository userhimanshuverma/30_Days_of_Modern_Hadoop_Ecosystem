#!/bin/bash
# Day 21 — Hive Execution Mode Validation: Apache Tez (Default)
# Location: Day-21-Hive-Execution-Modes/scripts/verify-tez.sh

set -e

HS2_HOST=${1:-"localhost"}
HS2_PORT=10000

echo "========================================================================="
echo "🔍 Starting Hive Apache Tez Execution Mode Verification"
echo "========================================================================="

# Submit query using Beeline and force Tez execution
echo "🚀 Submitting query with hive.execution.engine=tez..."
/opt/hive/bin/beeline -u "jdbc:hive2://$HS2_HOST:$HS2_PORT" -n hive -p hivepassword -e "
CREATE DATABASE IF NOT EXISTS verify_modes_db;
USE verify_modes_db;

CREATE TABLE IF NOT EXISTS test_tez_table (id INT, value STRING) STORED AS ORC;

INSERT OVERWRITE TABLE test_tez_table VALUES 
(1, 'Hive Tez DAG Execution 1'),
(2, 'Hive Tez DAG Execution 2'),
(3, 'Hive Tez DAG Execution 3'),
(4, 'Hive Tez DAG Execution 4');

-- Force Tez execution engine
SET hive.execution.engine=tez;
SET hive.cbo.enable=true;
SET hive.vectorized.execution.enabled=true;

-- Run aggregation to compile into a Tez DAG containing Map and Reduce stages
SELECT id, count(*) as count, max(value) as max_val 
FROM test_tez_table 
GROUP BY id;
"

echo "========================================================================="
echo "✅ Success: Query completed using Apache Tez engine!"
echo "========================================================================="
