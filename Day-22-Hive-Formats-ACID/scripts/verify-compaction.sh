#!/bin/bash
# Day 22 — verify-compaction.sh
# Location: Day-22-Hive-Formats-ACID/scripts/verify-compaction.sh

set -e

echo "=== [1/5] Starting Hive Compaction Verification Script ==="

# Hive connection settings
HS2_HOST="hiveserver2"
HS2_PORT="10000"
BEEline="beeline -u jdbc:hive2://${HS2_HOST}:${HS2_PORT}/default -n hive -p hivepassword"

echo "Checking HiveServer2 connectivity..."
until nc -z $HS2_HOST $HS2_PORT; do
  echo "Waiting for HiveServer2 to be ready..."
  sleep 3
done

# Ensure employee_acid has transactional data
if ! $BEEline -e "SELECT * FROM employee_acid LIMIT 1;" > /dev/null 2>&1; then
  echo "Error: employee_acid table is empty or does not exist. Run verify-acid.sh first!"
  exit 1
fi

echo "=== [2/5] Initial HDFS State (Before Compactions) ==="
hadoop fs -ls -R /user/hive/warehouse/employee_acid

echo "=== [3/5] Triggering MINOR Compaction ==="
echo "Minor compaction merges multiple delta files together, without removing deleted rows."
$BEEline -e "ALTER TABLE employee_acid COMPACT 'MINOR';"

echo "Current Compaction Status:"
$BEEline -e "SHOW COMPACTIONS;"

echo "=== [4/5] Triggering MAJOR Compaction ==="
echo "Major compaction merges all delta files and the base file into a single base directory, fully resolving updates and purging deleted rows."
$BEEline -e "ALTER TABLE employee_acid COMPACT 'MAJOR';"

echo "Current Compaction Status:"
$BEEline -e "SHOW COMPACTIONS;"

echo "=== [5/5] Monitoring Compaction Progress ==="
echo "Waiting for the asynchronous compactor workers to process the job..."
echo "Note: In a local single-node cluster, compaction may take up to a minute to transition from 'initiated' to 'working' and 'ready for cleaning'."

for i in {1..12}; do
  STATUS=$($BEEline --silent=true --outputformat=csv2 -e "SHOW COMPACTIONS;" | grep "employee_acid" | tail -n 1 2>/dev/null || echo "initiated")
  echo "Checking status (attempt $i/12): $STATUS"
  
  if [[ "$STATUS" == *"succeeded"* ]] || [[ "$STATUS" == *"ready for cleaning"* ]] || [[ "$STATUS" == *"attempted"* ]]; then
     echo "Compaction state changed: $STATUS"
     break
  fi
  sleep 5
done

echo "=== HDFS State (After Compaction Triggers) ==="
hadoop fs -ls -R /user/hive/warehouse/employee_acid

echo "Compaction verification script run finished."
echo "Inspect 'SHOW COMPACTIONS;' directly in beeline to verify cleaner daemon completion."
