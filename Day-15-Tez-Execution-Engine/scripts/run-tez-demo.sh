#!/bin/bash
# run-tez-demo.sh - Benchmarks Hive queries running on MapReduce vs. Apache Tez.

set -e

HIVE_HOST=${1:-"localhost"}
HIVE_PORT=${2:-"10000"}

echo "=========================================================================="
echo "🚀 BENCHMARK: HIVE ON MAPREDUCE VS. HIVE ON APACHE TEZ"
echo "=========================================================================="

echo "=== STEP 1: Pre-allocating a large dataset for testing ==="
beeline -u "jdbc:hive2://$HIVE_HOST:$HIVE_PORT/default" -n root -p "" -e "
CREATE TABLE IF NOT EXISTS benchmark_data (id INT, value STRING);
"

# Generate 20,000 rows in Hive using a join or cross join trick
echo "Generating rows..."
beeline -u "jdbc:hive2://$HIVE_HOST:$HIVE_PORT/default" -n root -p "" -e "
INSERT OVERWRITE TABLE benchmark_data
SELECT row_number() over() as id, 
       concat('value_', cast(rand()*100 as int)) as value 
FROM sample_lines t1 
CROSS JOIN sample_lines t2 
CROSS JOIN sample_lines t3 
CROSS JOIN sample_lines t4 
CROSS JOIN sample_lines t5
LIMIT 20000;
"

QUERY="SELECT value, count(*) as cnt, avg(id) as avg_id FROM benchmark_data GROUP BY value ORDER BY cnt DESC LIMIT 10;"

echo "=== STEP 2: Running query on MAPREDUCE engine ==="
START_MR=$(date +%s)
beeline -u "jdbc:hive2://$HIVE_HOST:$HIVE_PORT/default" -n root -p "" -e "
SET hive.execution.engine=mr;
$QUERY
"
END_MR=$(date +%s)
DURATION_MR=$((END_MR - START_MR))

echo "=== STEP 3: Running query on TEZ engine ==="
START_TEZ=$(date +%s)
beeline -u "jdbc:hive2://$HIVE_HOST:$HIVE_PORT/default" -n root -p "" -e "
SET hive.execution.engine=tez;
$QUERY
"
END_TEZ=$(date +%s)
DURATION_TEZ=$((END_TEZ - START_TEZ))

echo "=========================================================================="
echo "📊 BENCHMARK COMPARISON RESULTS"
echo "=========================================================================="
echo "  Query Execution Engine  |  Duration (seconds)"
echo "--------------------------------------------------------"
echo "  Hive on MapReduce       |  ${DURATION_MR}s"
echo "  Hive on Apache Tez      |  ${DURATION_TEZ}s"
echo "--------------------------------------------------------"

if [ $DURATION_TEZ -gt 0 ]; then
    SPEEDUP=$(echo "scale=2; $DURATION_MR / $DURATION_TEZ" | bc 2>/dev/null || echo "N/A")
    echo "⚡ Performance Gain: Tez is ${SPEEDUP}x faster than MapReduce!"
else
    echo "⚡ Tez execution was too fast to measure in whole seconds."
fi
echo "=========================================================================="
