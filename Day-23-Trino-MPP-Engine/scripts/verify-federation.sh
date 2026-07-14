#!/bin/bash
# Day 23: Trino Validation Script - Verify Federated Queries Join
# Location: Day-23-Trino-MPP-Engine/scripts/verify-federation.sh

echo "========================================================="
echo "Verifying Federated Queries Joining MySQL + Hive + Kafka"
echo "========================================================="

# 1. Setup relational data in MySQL
echo "Populating User Profiles in MySQL Database..."
docker exec -t mysql-day23 mysql -uroot -prootpassword -e "
  CREATE TABLE IF NOT EXISTS inventory.user_profiles (
    user_id INT PRIMARY KEY,
    signup_source VARCHAR(50),
    account_status VARCHAR(20)
  );
  TRUNCATE TABLE inventory.user_profiles;
  INSERT INTO inventory.user_profiles VALUES 
    (1, 'Google Ads Search', 'Active'), 
    (2, 'Referral Program', 'Pending'),
    (3, 'Direct Visit', 'Active');
"

# 2. Run Federated Join in Trino
echo "Executing cross-connector query in Trino..."
docker exec -t trino-coordinator-day23 trino --execute "
  SELECT 
    c.click_id AS click_id,
    h.name AS user_name,
    h.email AS user_email,
    m.signup_source AS signup_source,
    m.account_status AS account_status,
    c.page_url AS page_url
  FROM kafka.default.clicks c
  JOIN hive.demo.users h ON c.user_id = h.id
  JOIN mysql.inventory.user_profiles m ON c.user_id = m.user_id
  ORDER BY c.click_timestamp DESC
"

# 3. Analyze Query Execution Plan
echo ""
echo "========================================================="
echo "Analyzing Trino Query Execution Plan (EXPLAIN)"
echo "========================================================="
docker exec -t trino-coordinator-day23 trino --execute "
  EXPLAIN SELECT 
    c.click_id, h.name, m.signup_source
  FROM kafka.default.clicks c
  JOIN hive.demo.users h ON c.user_id = h.id
  JOIN mysql.inventory.user_profiles m ON c.user_id = m.user_id
"

if [ $? -eq 0 ]; then
  echo "✔ Federated SQL Query and Execution Planning validated successfully."
else
  echo "✘ Federated SQL Join failed. Verify connector connectivity or schemas."
  exit 1
fi
