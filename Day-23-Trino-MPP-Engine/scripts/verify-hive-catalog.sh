#!/bin/bash
# Day 23: Trino Validation Script - Verify Hive Catalog
# Location: Day-23-Trino-MPP-Engine/scripts/verify-hive-catalog.sh

echo "========================================================="
echo "Verifying Hive Metastore and HDFS Catalog in Trino"
echo "========================================================="

# 1. Create a Schema in Hive if it doesn't exist
docker exec -t trino-coordinator-day23 trino --execute "CREATE SCHEMA IF NOT EXISTS hive.demo"

# 2. Create a Table inside that Schema
echo "Creating Table hive.demo.users..."
docker exec -t trino-coordinator-day23 trino --execute "
  CREATE TABLE IF NOT EXISTS hive.demo.users (
    id INT,
    name VARCHAR,
    email VARCHAR
  ) WITH (
    format = 'ORC',
    external_location = 'hdfs://namenode-day23:9000/user/hive/warehouse/demo.db/users'
  )
"

# 3. Insert mock data
echo "Inserting record into hive.demo.users..."
docker exec -t trino-coordinator-day23 trino --execute "INSERT INTO hive.demo.users VALUES (1, 'Alice Smith', 'alice@example.com'), (2, 'Bob Jones', 'bob@example.com')"

# 4. Query the inserted records
echo "Querying hive.demo.users..."
docker exec -t trino-coordinator-day23 trino --execute "SELECT * FROM hive.demo.users"

if [ $? -eq 0 ]; then
  echo "✔ Hive Metastore catalog integration validated successfully."
else
  echo "✘ Failed to run Hive catalog queries. Review Trino/HMS container logs."
  exit 1
fi
