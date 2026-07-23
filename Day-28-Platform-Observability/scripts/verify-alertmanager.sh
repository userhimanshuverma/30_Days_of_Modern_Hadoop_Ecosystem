#!/usr/bin/env bash
# scripts/verify-alertmanager.sh
# Verification script for Alertmanager health and alert status

set -eo pipefail

echo "========================================================="
echo "🔍 DIAGNOSTIC REPORT: ALERTMANAGER ROUTING SERVICE"
echo "========================================================="

ALERT_URL=${1:-"http://localhost:9093"}

if curl -s -f "${ALERT_URL}/-/healthy" >/dev/null; then
    echo "🟢 Alertmanager Engine (${ALERT_URL}): ONLINE"
else
    echo "🔴 Alertmanager Engine (${ALERT_URL}): OFFLINE"
    exit 1
fi

echo "🔍 Fetching active firing alerts..."
ALERTS=$(curl -s "${ALERT_URL}/api/v2/alerts")
ALERT_COUNT=$(echo "$ALERTS" | grep -o '"status"' | wc -l || echo "0")
echo "📊 Current Active Alerts in Alertmanager: ${ALERT_COUNT}"

echo "========================================================="
echo "Alertmanager verification complete."
