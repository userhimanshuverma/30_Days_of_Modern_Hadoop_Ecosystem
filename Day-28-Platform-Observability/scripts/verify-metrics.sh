#!/usr/bin/env bash
# scripts/verify-metrics.sh
# Verification script using PromQL queries against Prometheus API

set -eo pipefail

echo "========================================================="
echo "🔍 DIAGNOSTIC REPORT: PROMQL METRIC INGESTION QUERY TEST"
echo "========================================================="

PROM_URL=${1:-"http://localhost:9090"}

query_promql() {
    local label=$1
    local query=$2
    echo "🔍 Testing PromQL: ${label} ('${query}')..."
    RESPONSE=$(curl -s --data-urlencode "query=${query}" "${PROM_URL}/api/v1/query")
    if echo "$RESPONSE" | grep -q '"status":"success"'; then
        echo "🟢 Query Successful: ${label}"
    else
        echo "🔴 Query Failed: ${label}"
    fi
}

query_promql "Node CPU Idle Seconds" "node_cpu_seconds_total{mode='idle'}"
query_promql "Host Memory Available Bytes" "node_memory_MemAvailable_bytes"
query_promql "Prometheus Scrape Health (up)" "up"

echo "========================================================="
echo "Metric PromQL evaluation complete."
