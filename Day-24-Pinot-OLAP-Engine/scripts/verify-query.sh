#!/usr/bin/env bash

# Exit immediately if a query fails or errors
set -e

echo "======================================================================"
echo "⚡ Executing SQL Queries Against Pinot Broker"
echo "======================================================================"

# Verify Broker is responsive
if ! curl -s -o /dev/null -w "%{http_code}" http://localhost:8099/health | grep -q "200"; then
    echo "❌ Error: Pinot Broker is not reachable on http://localhost:8099"
    exit 1
fi

# Check if jq is installed
if command -v jq >/dev/null 2>&1; then
    HAS_JQ=true
else
    HAS_JQ=false
fi

execute_query() {
    local title=$1
    local query=$2

    echo ""
    echo "--------------------------------------------------------"
    echo "🔍 Query: $title"
    echo "SQL: $query"
    echo "--------------------------------------------------------"

    # Send POST request to Pinot Broker
    if [ "$HAS_JQ" = true ]; then
        curl -s -X POST -H 'Content-Type: application/json' \
             -d "{\"sql\":\"$query\"}" \
             http://localhost:8099/query/sql | jq .
    else
        curl -s -X POST -H 'Content-Type: application/json' \
             -d "{\"sql\":\"$query\"}" \
             http://localhost:8099/query/sql
        echo "" # Add newline
    fi
}

# 1. Count of all registrations
execute_query "Total User Registrations" \
              "SELECT COUNT(*) FROM user_registrations"

# 2. Total fee grouped by account type (aggregating metrics)
execute_query "Total Registration Fee Revenue by Account Type" \
              "SELECT accountType, COUNT(*), SUM(registrationFee) FROM user_registrations GROUP BY accountType ORDER BY SUM(registrationFee) DESC"

# 3. Filtered query utilizing indexes (Bloom filter / Range index)
execute_query "Premium Users Over 25 Years Old" \
              "SELECT userId, username, country, age, accountType FROM user_registrations WHERE accountType = 'PREMIUM' AND age > 25 LIMIT 5"

# 4. Text index matching (utilizes text index on email field)
execute_query "Searching Gmail Users using Text Match Index" \
              "SELECT username, email, country FROM user_registrations WHERE TEXT_MATCH(email, '*gmail*') LIMIT 5"

# 5. Segment metadata query
execute_query "Querying Ingestion Statistics & Latency Metadata" \
              "SELECT COUNT(*), MIN(signupTimestamp), MAX(signupTimestamp) FROM user_registrations"

echo ""
echo "======================================================================"
echo "🎉 All queries executed successfully!"
echo "======================================================================"
