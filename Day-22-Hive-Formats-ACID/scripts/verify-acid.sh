#!/bin/bash
# Day 22 — verify-acid.sh
# Location: Day-22-Hive-Formats-ACID/scripts/verify-acid.sh

set -e

echo "=== [1/6] Starting Hive ACID Verification Script ==="

# Hive connection settings
HS2_HOST="hiveserver2"
HS2_PORT="10000"
BEEline="beeline -u jdbc:hive2://${HS2_HOST}:${HS2_PORT}/default -n hive -p hivepassword"

echo "Checking HiveServer2 connectivity..."
until nc -z $HS2_HOST $HS2_PORT; do
  echo "Waiting for HiveServer2 to be ready..."
  sleep 3
done

echo "=== [2/6] Creating Managed ACID Table (STORED AS ORC) ==="
$BEEline -e "
DROP TABLE IF EXISTS employee_acid;
CREATE TABLE employee_acid (
    id INT,
    name STRING,
    salary DOUBLE,
    role STRING
)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');
"

echo "=== [3/6] Step 1: INSERT Operations ==="
$BEEline -e "
INSERT INTO employee_acid VALUES 
(1, 'Alice Smith', 120000.0, 'Staff Engineer'),
(2, 'Bob Jones', 95000.0, 'Senior Engineer'),
(3, 'Charlie Brown', 75000.0, 'Software Engineer');
"

echo "Current HDFS File Layout after INSERT (Should contain delta_0000001_0000001):"
hadoop fs -ls -R /user/hive/warehouse/employee_acid

echo "=== [4/6] Step 2: UPDATE Operations ==="
# Update Alice's salary
$BEEline -e "
UPDATE employee_acid SET salary = 130000.0 WHERE id = 1;
"

echo "Current HDFS File Layout after UPDATE (Should contain delta_0000002_0000002 and delete_delta_0000002_0000002):"
hadoop fs -ls -R /user/hive/warehouse/employee_acid

echo "=== [5/6] Step 3: DELETE Operations ==="
# Delete Charlie Brown
$BEEline -e "
DELETE FROM employee_acid WHERE id = 3;
"

echo "Current HDFS File Layout after DELETE (Should contain delete_delta_0000003_0000003):"
hadoop fs -ls -R /user/hive/warehouse/employee_acid

echo "=== [6/6] Step 4: MERGE Operations ==="
# Create staging table for MERGE demonstration
$BEEline -e "
DROP TABLE IF EXISTS employee_stage;
CREATE TABLE employee_stage (
    id INT,
    name STRING,
    salary DOUBLE,
    role STRING,
    action STRING
);

INSERT INTO employee_stage VALUES
(1, 'Alice Smith', 140000.0, 'Principal Engineer', 'UPDATE'), -- Update Salary/Role
(2, 'Bob Jones', 95000.0, 'Senior Engineer', 'DELETE'),       -- Delete Bob
(4, 'Diana Prince', 85000.0, 'Marketing Manager', 'INSERT');   -- Insert Diana
"

# Execute MERGE
$BEEline -e "
MERGE INTO employee_acid AS t USING employee_stage AS s ON t.id = s.id
WHEN MATCHED AND s.action = 'DELETE' THEN DELETE
WHEN MATCHED AND s.action = 'UPDATE' THEN UPDATE SET salary = s.salary, role = s.role
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.salary, s.role);
"

echo "=== Final Query Results (Snapshot Isolation Read) ==="
$BEEline -e "
SELECT * FROM employee_acid ORDER BY id;
"

echo "Current HDFS File Layout after MERGE:"
hadoop fs -ls -R /user/hive/warehouse/employee_acid

echo "=== ACID Verification Complete ==="
