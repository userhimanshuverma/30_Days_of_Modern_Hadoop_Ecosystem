#!/usr/bin/env bash
# =========================================================================
# Hive SQL Query Engine Validation Script - Day 1
# Connects to HiveServer2 via Beeline and runs test SQL statements.
# =========================================================================

set -euo pipefail

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SERVER_CONTAINER="hive-server-day01"
METASTORE_CONTAINER="hive-metastore-day01"
DB_CONTAINER="hive-metastore-db-day01"

echo -e "${YELLOW}=== Starting Hive Query Engine Validation ===${NC}"

# 1. Check if containers are running
for container in "${DB_CONTAINER}" "${METASTORE_CONTAINER}" "${SERVER_CONTAINER}"; do
  if ! docker ps --filter "name=${container}" --filter "status=running" | grep -q "${container}"; then
    echo -e "${RED}[ERROR] Hive service container '${container}' is not running.${NC}"
    exit 1
  fi
done
echo -e "${GREEN}[OK] Hive DB, Metastore, and Server2 containers are running.${NC}"

# 2. Wait for HiveServer2 to start up (typically takes some time)
echo "Waiting for HiveServer2 (Port 10000) to accept connections (this can take up to 2 minutes)..."
for i in {1..60}; do
  # Run a trivial query via beeline to check if HS2 is accepting connections
  if docker exec "${SERVER_CONTAINER}" beeline -u "jdbc:hive2://localhost:10000" -n hive -p hive -e "SHOW DATABASES;" >/dev/null 2>&1; then
    echo -e "${GREEN}[OK] HiveServer2 is online and accepting JDBC connections!${NC}"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo -e "${RED}[ERROR] HiveServer2 failed to start within 2 minutes.${NC}"
    echo -e "${YELLOW}Check container logs: docker logs ${SERVER_CONTAINER}${NC}"
    exit 1
  fi
  echo -n "."
  sleep 4
done
echo ""

# 3. Perform Schema and Data Write/Read Test
echo "Running schema design and query execution tests..."
SQL_TEST_COMMANDS=$(cat << 'EOF'
CREATE DATABASE IF NOT EXISTS day01_db;
USE day01_db;
CREATE TABLE IF NOT EXISTS validation_table (id INT, message STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
INSERT INTO TABLE validation_table VALUES (1, 'Hive-HDFS-Metastore-Integration-Test-Success');
SELECT * FROM validation_table;
DROP TABLE validation_table;
DROP DATABASE day01_db;
EOF
)

# Execute queries
echo "Executing test queries in HiveServer2..."
QUERY_OUTPUT=$(docker exec -i "${SERVER_CONTAINER}" beeline -u "jdbc:hive2://localhost:10000" -n hive -p hive -e "${SQL_TEST_COMMANDS}" 2>&1 || true)

# 4. Check Query Result
if echo "$QUERY_OUTPUT" | grep -q "Hive-HDFS-Metastore-Integration-Test-Success"; then
  echo -e "${GREEN}[OK] Hive query test successful. Table created, data inserted, queried, and cleaned up.${NC}"
  echo -e "${GREEN}=== Hive Query Engine Validation PASSED successfully! ===${NC}"
else
  echo -e "${RED}[ERROR] Hive query test failed. Output did not contain expected test result.${NC}"
  echo -e "${YELLOW}--- HIVE QUERY OUTPUT ---${NC}"
  echo "$QUERY_OUTPUT"
  echo -e "${YELLOW}-------------------------${NC}"
  exit 1
fi
