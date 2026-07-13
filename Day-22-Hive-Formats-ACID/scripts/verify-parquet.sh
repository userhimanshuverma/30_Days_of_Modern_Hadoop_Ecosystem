#!/bin/bash
# Day 22 — verify-parquet.sh
# Location: Day-22-Hive-Formats-ACID/scripts/verify-parquet.sh

set -e

echo "=== [1/5] Starting Hive Parquet Verification Script ==="

# Hive connection settings
HS2_HOST="hiveserver2"
HS2_PORT="10000"
BEEline="beeline -u jdbc:hive2://${HS2_HOST}:${HS2_PORT}/default -n hive -p hivepassword"

echo "Checking HiveServer2 connectivity..."
until nc -z $HS2_HOST $HS2_PORT; do
  echo "Waiting for HiveServer2 to be ready..."
  sleep 3
done

echo "=== [2/5] Creating Parquet Table with Optimizations ==="
$BEEline -e "
DROP TABLE IF EXISTS employee_parquet;
CREATE TABLE employee_parquet (
    id INT,
    name STRING,
    salary DOUBLE,
    role STRING
)
PARTITIONED BY (dept STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'parquet.enable.dictionary'='true'
);
"

echo "=== [3/5] Loading Sample Data ==="
$BEEline -e "
INSERT INTO TABLE employee_parquet PARTITION(dept='Engineering') VALUES 
(1, 'Alice Smith', 120000.0, 'Staff Engineer'),
(2, 'Bob Jones', 95000.0, 'Senior Engineer'),
(3, 'Charlie Brown', 75000.0, 'Software Engineer');

INSERT INTO TABLE employee_parquet PARTITION(dept='Marketing') VALUES 
(4, 'Diana Prince', 85000.0, 'Marketing Manager'),
(5, 'Evan Wright', 60000.0, 'Marketing Analyst');
"

echo "=== [4/5] Verifying Partition Pruning & Query Performance ==="
$BEEline -e "
SELECT name, role FROM employee_parquet WHERE dept='Engineering' AND salary > 80000;
"

echo "=== [5/5] Checking Parquet File Layout on HDFS ==="
echo "Listing HDFS Directory contents for employee_parquet:"
hadoop fs -ls -R /user/hive/warehouse/employee_parquet

echo "=== Parquet Verification Complete ==="
