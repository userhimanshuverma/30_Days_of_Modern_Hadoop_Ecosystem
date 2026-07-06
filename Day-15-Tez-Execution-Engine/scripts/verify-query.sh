#!/bin/bash
# verify-query.sh - Runs a sample Hive query on Tez and parses logs to ensure DAG-based execution is working.

set -e

HIVE_HOST=${1:-"localhost"}
HIVE_PORT=${2:-"10000"}

echo "=== 🔍 STEP 1: Creating staging table and adding sample data ==="
beeline -u "jdbc:hive2://$HIVE_HOST:$HIVE_PORT/default" -n root -p "" -e "
CREATE TABLE IF NOT EXISTS sample_lines (line STRING);
INSERT OVERWRITE TABLE sample_lines VALUES 
('apache tez is a dag engine'),
('hadoop mapreduce writes to disk between jobs'),
('tez pipelines tasks in memory'),
('hive on tez is faster than hive on mapreduce'),
('distributing analytics pipelines on yarn');
"

echo "=== 🔍 STEP 2: Running aggregations using Hive-on-Tez ==="
echo "Executing aggregation query. Watch for Tez DAG vertex messages in logs..."

LOG_FILE="/tmp/hive_tez_query.log"
beeline -u "jdbc:hive2://$HIVE_HOST:$HIVE_PORT/default" -n root -p "" -e "
SELECT word, count(*) as count 
FROM (
  SELECT explode(split(line, ' ')) as word FROM sample_lines
) temp 
GROUP BY word 
ORDER BY count DESC 
LIMIT 5;
" > "$LOG_FILE" 2>&1

cat "$LOG_FILE"

echo "=== 🔍 STEP 3: Parsing Execution Log to Confirm Tez DAG Vertices ==="
if grep -q "Tez" "$LOG_FILE" || grep -q "DAG" "$LOG_FILE" || grep -q "Map 1" "$LOG_FILE"; then
    echo "✅ Success: Confirmed Tez DAG execution was utilized."
    echo "Summary of Vertices:"
    grep -E "Map 1|Reducer 2|Map|Reducer|DAG" "$LOG_FILE" || true
else
    echo "❌ Error: Could not find Tez execution signatures in logs. Double check settings."
    exit 1
fi

echo "=== 🎉 Query Verification Completed Successfully! ==="
