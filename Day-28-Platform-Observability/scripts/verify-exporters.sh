#!/usr/bin/env bash
# scripts/verify-exporters.sh
# Verification script for Exporter HTTP /metrics endpoints

set -eo pipefail

echo "========================================================="
echo "🔍 DIAGNOSTIC REPORT: NODE & JMX EXPORTERS"
echo "========================================================="

check_exporter() {
    local name=$1
    local url=$2
    if curl -s -f "$url" | grep -q "^# HELP"; then
        echo "🟢 ${name} (${url}): ONLINE (Serving valid OpenMetrics)"
    else
        echo "🟡 ${name} (${url}): UNREACHABLE / NO METRICS"
    fi
}

check_exporter "Node Exporter" "http://localhost:9100/metrics"
check_exporter "Kafka JMX Exporter" "http://localhost:5558/metrics"
check_exporter "HDFS NameNode JMX" "http://localhost:5556/metrics"
check_exporter "HDFS DataNode JMX" "http://localhost:5557/metrics"
check_exporter "Spark Driver Metrics" "http://localhost:4040/metrics/prometheus"
check_exporter "Trino Telemetry" "http://localhost:8081/metrics"
check_exporter "Pinot Controller Exporter" "http://localhost:8082/metrics"

echo "========================================================="
echo "Exporter check complete."
