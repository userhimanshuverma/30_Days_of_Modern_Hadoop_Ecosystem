#!/bin/bash
# Day 20: Hive Managed and External Table Verification Script
# Location: Day-20-Hive-Architecture-HMS/scripts/verify-tables.sh

set -e

HS2_HOST=${1:-"localhost"}
HS2_PORT=10000

echo "=== 🔍 STEP 1: Creating Local CSV data for External Table testing ==="
mkdir -p /tmp/hive_test_data
cat <<EOF > /tmp/hive_test_data/logs.csv
101,INFO,System initialized successfully
102,WARNING,Connection pool size reached 80%
103,ERROR,NullPointerException in execution flow
104,INFO,Cleanup task completed
EOF

echo "=== 🔍 STEP 2: uploading CSV to HDFS location ==="
/opt/hadoop/bin/hadoop fs -mkdir -p /tmp/verify_logs
/opt/hadoop/bin/hadoop fs -put -f /tmp/hive_test_data/logs.csv /tmp/verify_logs/
echo "✅ Success: Uploaded CSV to HDFS /tmp/verify_logs/logs.csv."

echo "=== 🔍 STEP 3: Initializing Database & Tables (Managed & External) ==="
/opt/hive/bin/beeline -u "jdbc:hive2://$HS2_HOST:$HS2_PORT" -n hive -p hive -e "
CREATE DATABASE IF NOT EXISTS verify_db;
USE verify_db;

-- Managed Table (ORC format)
CREATE TABLE IF NOT EXISTS managed_users (
    id INT,
    name STRING
) STORED AS ORC;

-- External Table (Text CSV format)
CREATE EXTERNAL TABLE IF NOT EXISTS external_logs (
    log_id INT,
    log_level STRING,
    message STRING
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION '/tmp/verify_logs';
"
echo "✅ Success: Tables created."

echo "=== 🔍 STEP 4: Inserting records into Managed Table ==="
/opt/hive/bin/beeline -u "jdbc:hive2://$HS2_HOST:$HS2_PORT" -n hive -p hive -e "
USE verify_db;
INSERT INTO managed_users VALUES 
(1, 'Alice'),
(2, 'Bob'),
(3, 'Charlie');
"
echo "✅ Success: Data inserted."

echo "=== 🔍 STEP 5: Verifying HDFS storage locations ==="
echo "📁 Managed Table Warehouse Path:"
/opt/hadoop/bin/hadoop fs -ls /user/hive/warehouse/verify_db.db/managed_users/
echo "📁 External Table Path:"
/opt/hadoop/bin/hadoop fs -ls /tmp/verify_logs/

echo "=== 🔍 STEP 6: Running analytical queries ==="
echo "📊 Managed Table Row Count (Aggregation):"
/opt/hive/bin/beeline -u "jdbc:hive2://$HS2_HOST:$HS2_PORT" -n hive -p hive -e "
USE verify_db;
SELECT COUNT(*) AS total_users FROM managed_users;
"

echo "📊 External Table Filtering (Only ERROR logs):"
/opt/hive/bin/beeline -u "jdbc:hive2://$HS2_HOST:$HS2_PORT" -n hive -p hive -e "
USE verify_db;
SELECT * FROM external_logs WHERE log_level = 'ERROR';
"

echo "=== 🔍 STEP 7: Testing DROP behavior (Managed vs External metadata) ==="
/opt/hive/bin/beeline -u "jdbc:hive2://$HS2_HOST:$HS2_PORT" -n hive -p hive -e "
USE verify_db;
DROP TABLE managed_users;
DROP TABLE external_logs;
DROP DATABASE verify_db;
"

echo "🔍 Checking HDFS paths after dropping tables..."
echo "📁 Checking Managed Table storage (Should be deleted):"
if /opt/hadoop/bin/hadoop fs -test -e /user/hive/warehouse/verify_db.db/managed_users/; then
  echo "❌ Error: Managed table directory still exists after DROP."
  exit 1
else
  echo "✅ Success: Managed table HDFS directory was deleted automatically."
fi

echo "📁 Checking External Table storage (Should still exist):"
if /opt/hadoop/bin/hadoop fs -test -f /tmp/verify_logs/logs.csv; then
  echo "✅ Success: External table HDFS data was preserved."
else
  echo "❌ Error: External table HDFS directory was deleted."
  exit 1
fi

# Cleanup HDFS test file and local files
/opt/hadoop/bin/hadoop fs -rm -r -f /tmp/verify_logs
rm -rf /tmp/hive_test_data

echo "=== 🎉 Table Operations & DDL Verification Passed! ==="
