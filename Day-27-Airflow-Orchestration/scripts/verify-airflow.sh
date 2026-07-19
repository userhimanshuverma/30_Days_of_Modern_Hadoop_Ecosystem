#!/usr/bin/env bash
# scripts/verify-airflow.sh
# Diagnostic script to check health of Airflow containers and network ports

set -eo pipefail

echo "========================================================="
echo "🔍 DIAGNOSTIC REPORT: APACHE AIRFLOW CORE NETWORK PORTS"
echo "========================================================="

# 1. Check PostgreSQL Database Port
if nc -z localhost 5432 2>/dev/null; then
    echo "🟢 Postgres (port 5432): ONLINE"
else
    echo "🔴 Postgres (port 5432): OFFLINE (Check database container)"
fi

# 2. Check Redis Broker Port
if nc -z localhost 6379 2>/dev/null; then
    echo "🟢 Redis Broker (port 6379): ONLINE"
else
    echo "🔴 Redis Broker (port 6379): OFFLINE (Check redis container)"
fi

# 3. Check Airflow Webserver Port
if nc -z localhost 8080 2>/dev/null; then
    echo "🟢 Webserver UI (port 8080): ONLINE"
    
    # Query the webserver health endpoint
    echo "🔍 Querying http://localhost:8080/health..."
    HEALTH_STATUS=$(curl -s http://localhost:8080/health)
    echo "Status Payload: $HEALTH_STATUS"
else
    echo "🔴 Webserver UI (port 8080): OFFLINE"
fi

# 4. Check Celery Worker default log server
if nc -z localhost 8793 2>/dev/null; then
    echo "🟢 Celery Worker Log Server (port 8793): ONLINE"
else
    echo "🟡 Celery Worker Log Server (port 8793): UNREACHABLE (Normal if using LocalExecutor)"
fi

echo "========================================================="
echo "Report evaluation complete."
