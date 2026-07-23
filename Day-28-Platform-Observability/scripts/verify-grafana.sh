#!/usr/bin/env bash
# scripts/verify-grafana.sh
# Verification script for Grafana Server status and datasource provisioning

set -eo pipefail

echo "========================================================="
echo "🔍 DIAGNOSTIC REPORT: GRAFANA DASHBOARD SERVER"
echo "========================================================="

GRAFANA_URL=${1:-"http://localhost:3000"}

if curl -s -f "${GRAFANA_URL}/api/health" >/dev/null; then
    echo "🟢 Grafana Web Engine (${GRAFANA_URL}): ONLINE & HEALTHY"
else
    echo "🔴 Grafana Web Engine (${GRAFANA_URL}): OFFLINE"
    exit 1
fi

echo "🔍 Validating Datasources via REST API..."
DATASOURCES=$(curl -s -u admin:admin "${GRAFANA_URL}/api/datasources" 2>/dev/null || echo "[]")
if echo "$DATASOURCES" | grep -q "Prometheus"; then
    echo "🟢 Prometheus Datasource: PROVISIONED & VERIFIED"
else
    echo "🟡 Prometheus Datasource: NOT DETECTED (Check provisioning configs)"
fi

echo "========================================================="
echo "Grafana verification complete."
