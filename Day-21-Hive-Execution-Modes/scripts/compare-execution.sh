#!/bin/bash
# Day 21 — Hive Execution Engine Performance Comparison
# Location: Day-21-Hive-Execution-Modes/scripts/compare-execution.sh

set -e

HS2_HOST=${1:-"localhost"}
HS2_PORT=10000

echo "========================================================================="
echo "📊 Hive Execution Engines Performance Comparison Benchmark"
echo "========================================================================="

# Step 1: Set up the benchmark database and seed dataset
echo "🧹 Initializing benchmark database and generating test dataset (10,000 rows)..."
/opt/hive/bin/beeline -u "jdbc:hive2://$HS2_HOST:$HS2_PORT" -n hive -p hivepassword -e "
CREATE DATABASE IF NOT EXISTS benchmark_modes_db;
USE benchmark_modes_db;

-- 1. Create a seed table
DROP TABLE IF EXISTS seed_table;
CREATE TABLE seed_table (id INT, val DOUBLE) STORED AS TEXTFILE;
INSERT INTO seed_table VALUES 
(1, 12.5), (2, 8.4), (3, 19.1), (4, 4.3), (5, 10.0), 
(6, 15.6), (7, 25.1), (8, 3.2), (9, 14.8), (10, 20.0);

-- 2. Create the larger benchmark table using cross joins (10^4 = 10,000 rows)
DROP TABLE IF EXISTS benchmark_logs;
CREATE TABLE benchmark_logs STORED AS ORC AS
SELECT 
    (s1.id + s2.id * 10 + s3.id * 100) AS log_id,
    CASE 
        WHEN (s1.id % 3 = 0) THEN 'ERROR'
        WHEN (s1.id % 3 = 1) THEN 'INFO'
        ELSE 'WARN'
    END AS log_level,
    (s1.val * 1.5 + s2.val * 0.8 + s3.val * 0.2 + s4.val * 0.1) AS response_time
FROM seed_table s1
CROSS JOIN seed_table s2
CROSS JOIN seed_table s3
CROSS JOIN seed_table s4;
"
echo "✅ Test dataset populated in benchmark_modes_db.benchmark_logs!"

# Define the analytical query to run
BENCHMARK_QUERY="SELECT log_level, COUNT(*), AVG(response_time), SUM(response_time) FROM benchmark_logs GROUP BY log_level;"

# Helper function to run the query and measure time
run_benchmark() {
    local engine=$1
    echo "------------------------------------------------------------------------"
    echo "⚡ Running benchmark query using execution engine: ${engine^^}..."
    echo "------------------------------------------------------------------------"
    
    local start_time=$(date +%s.%N)
    
    # Run the query setting the execution engine
    /opt/hive/bin/beeline -u "jdbc:hive2://$HS2_HOST:$HS2_PORT" -n hive -p hivepassword -e "
    USE benchmark_modes_db;
    SET hive.execution.engine=$engine;
    SET hive.cbo.enable=true;
    $BENCHMARK_QUERY
    " > /dev/null 2>&1
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - start_time" | bc)
    echo "⏱️ Execution Time for ${engine^^}: ${duration} seconds"
    eval "${engine}_time=\$duration"
}

# Run benchmarks for all three engines
run_benchmark "mr"
run_benchmark "tez"
run_benchmark "spark"

# Print comparison summary table
echo "========================================================================="
echo "📊 BENCHMARK EXECUTION SUMMARY"
echo "========================================================================="
printf "| %-15s | %-18s | %-12s |\n" "Execution Engine" "Time (seconds)" "Relative Speed"
printf "|-----------------|--------------------|--------------|\n"

# Compute speedup multipliers
tez_speedup=$(echo "scale=2; $mr_time / $tez_time" | bc)
spark_speedup=$(echo "scale=2; $mr_time / $spark_time" | bc)

printf "| %-15s | %-18.3f | %-12s |\n" "MapReduce (MR)" "$mr_time" "1.00x (Baseline)"
printf "| %-15s | %-18.3f | %-12.2fx |\n" "Apache Tez" "$tez_time" "$tez_speedup"
printf "| %-15s | %-18.3f | %-12.2fx |\n" "Apache Spark" "$spark_time" "$spark_speedup"
echo "========================================================================="

# Clean up benchmark database
echo "🧹 Cleaning up database..."
/opt/hive/bin/beeline -u "jdbc:hive2://$HS2_HOST:$HS2_PORT" -n hive -p hivepassword -e "
DROP TABLE IF EXISTS benchmark_modes_db.benchmark_logs;
DROP TABLE IF EXISTS benchmark_modes_db.seed_table;
DROP DATABASE IF EXISTS benchmark_modes_db;
" > /dev/null 2>&1
echo "✅ Cleanup complete!"
