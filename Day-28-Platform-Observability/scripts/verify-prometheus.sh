#!/usr/bin/env bash
# scripts/verify-prometheus.sh
# Verification script to validate Prometheus server status, health, and scrape targets

set -eo pipefail

echo "========================================================="
echo "🔍 DIAGNOSTIC REPORT: PROMETHEUS SERVER HEALTH & TARGETS"
echo "========================================================="

PROM_URL=${1:-"http://localhost:9090"}

# 1. Check TCP Port and Healthy Endpoint
if curl -s -f "${PROM_URL}/-/healthy" >/dev/null; then
    echo "🟢 Prometheus Engine (${PROM_URL}): HEALTHY"
else
    echo "🔴 Prometheus Engine (${PROM_URL}): UNHEALTHY / OFFLINE"
    exit 1
fi

# 2. Check Ready Endpoint
if curl -s -f "${PROM_URL}/-/ready" >/dev/null; then
    echo "🟢 Prometheus TSDB Storage: READY"
else
    echo "🟡 Prometheus TSDB Storage: RECOVERING / NOT READY"
fi

# 3. Query Scrape Targets via API
echo "🔍 Querying Prometheus Scrape Targets..."
TARGETS_JSON=$(curl -s "${PROM_URL}/api/v1/targets")
ACTIVE_TARGETS=$(echo "$TARGETS_JSON" | grep -o '"health":"up"' | wc -l || echo "0")

echo "📊 Total Active Healthy Scrape Targets: ${ACTIVE_TARGETS}"
echo "========================================================="
echo "Prometheus verification complete."
