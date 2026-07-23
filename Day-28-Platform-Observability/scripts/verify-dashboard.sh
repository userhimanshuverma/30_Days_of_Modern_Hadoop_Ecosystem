#!/usr/bin/env bash
# scripts/verify-dashboard.sh
# Verification script for Grafana JSON dashboard syntax validity

set -eo pipefail

echo "========================================================="
echo "🔍 DIAGNOSTIC REPORT: GRAFANA DASHBOARD JSON VALIDATION"
echo "========================================================="

DASHBOARD_DIR=${1:-"./dashboards"}

for file in "${DASHBOARD_DIR}"/*.json; do
    if [ -f "$file" ]; then
        if python3 -m json.tool "$file" > /dev/null 2>&1 || python -m json.tool "$file" > /dev/null 2>&1; then
            echo "🟢 Dashboard Syntax Valid: $(basename "$file")"
        else
            echo "🔴 Dashboard Syntax INVALID: $(basename "$file")"
        fi
    fi
done

echo "========================================================="
echo "Dashboard JSON verification complete."
