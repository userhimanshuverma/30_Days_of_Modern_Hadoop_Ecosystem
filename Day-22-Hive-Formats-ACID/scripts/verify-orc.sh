#!/bin/bash
# Day 22 — verify-orc.sh
# Location: Day-22-Hive-Formats-ACID/scripts/verify-orc.sh

set -e

echo "=== [1/5] Starting Hive ORC Verification Script ==="

# Hive connection settings
HS2_HOST="hiveserver2"
HS2_PORT="10000"
BEEline="beeline -u jdbc:hive2://${HS2_HOST}:${HS2_PORT}/default -n hive -p hivepassword"

echo "Checking HiveServer2 connectivity..."
until nc -z $HS2_HOST $HS2_PORT; do
  echo "Waiting for HiveServer2 to be ready..."
  sleep 3
done

echo "=== [2/5] Creating ORC Table with Optimizations ==="
$BEEline -e "
DROP TABLE IF EXISTS employee_orc;
CREATE TABLE employee_orc (
    id INT,
    name STRING,
    salary DOUBLE,
    role STRING
)
PARTITIONED BY (dept STRING)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'orc.stripe.size'='67108864',
    'orc.row.index.stride'='10000',
    'orc.create.index'='true'
);
"

echo "=== [3/5] Loading Sample Data ==="
$BEEline -e "
INSERT INTO TABLE employee_orc PARTITION(dept='Engineering') VALUES 
(1, 'Alice Smith', 120000.0, 'Staff Engineer'),
(2, 'Bob Jones', 95000.0, 'Senior Engineer'),
(3, 'Charlie Brown', 75000.0, 'Software Engineer');

INSERT INTO TABLE employee_orc PARTITION(dept='Marketing') VALUES 
(4, 'Diana Prince', 85000.0, 'Marketing Manager'),
(5, 'Evan Wright', 60000.0, 'Marketing Analyst');
"

echo "=== [4/5] Verifying Partition Pruning & Column Pruning (Explain Plan) ==="
# We select only name and salary from Engineering dept. Explain plan should show column and partition pruning.
$BEEline -e "
EXPLAIN DEPENDENCIES SELECT name, salary FROM employee_orc WHERE dept='Engineering';
"

echo "=== [5/5] Performing ORC File Dump to Inspect Stripes & Row Groups ==="
# Locate HDFS directory of the table
HDFS_PATH=$(hadoop fs -ls /user/hive/warehouse/employee_orc/dept=Engineering | grep -o '/user/hive/warehouse/employee_orc/dept=Engineering/[^ ]*' | head -n 1)

echo "Located HDFS file: $HDFS_PATH"

# Run Hive's built-in ORC metadata dump tool
echo "Dumping ORC file structures using Hive ORC file dump..."
hive --service orcdump "$HDFS_PATH" | head -n 50

echo "=== ORC Verification Complete ==="
