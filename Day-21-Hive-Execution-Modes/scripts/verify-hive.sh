#!/bin/bash
# Day 21 — Hive Execution Mode Validation: MapReduce (Legacy)
# Location: Day-21-Hive-Execution-Modes/scripts/verify-hive.sh

set -e

HS2_HOST=${1:-"localhost"}
HS2_PORT=10000

echo "========================================================================="
echo "🔍 Starting Hive MapReduce (MR) Execution Mode Verification"
echo "========================================================================="

# Submit query using Beeline and force MapReduce execution
echo "🚀 Submitting query with hive.execution.engine=mr..."
/opt/hive/bin/beeline -u "jdbc:hive2://$HS2_HOST:$HS2_PORT" -n hive -p hivepassword -e "
CREATE DATABASE IF NOT EXISTS verify_modes_db;
USE verify_modes_db;

CREATE TABLE IF NOT EXISTS test_mr_table (id INT, value STRING) STORED AS ORC;

INSERT OVERWRITE TABLE test_mr_table VALUES 
(1, 'Hive MapReduce Execution 1'),
(2, 'Hive MapReduce Execution 2'),
(3, 'Hive MapReduce Execution 3'),
(4, 'Hive MapReduce Execution 4');

-- Force MapReduce execution engine
SET hive.execution.engine=mr;
SET hive.cbo.enable=true;

-- Run aggregation to force a Map & Reduce phase
SELECT id, count(*) as count, max(value) as max_val 
FROM test_mr_table 
GROUP BY id;
"

echo "========================================================================="
echo "✅ Success: Query completed using MapReduce engine!"
echo "========================================================================="
