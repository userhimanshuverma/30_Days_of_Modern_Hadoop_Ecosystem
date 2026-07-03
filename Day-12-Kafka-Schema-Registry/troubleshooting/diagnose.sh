#!/usr/bin/env bash
# diagnose.sh
# Diagnostic utility to troubleshoot local Kafka and Schema Registry issues.

set -euo pipefail

REGISTRY_URL="http://localhost:8081"
KAFKA_BROKER="localhost:19092"

echo "========================================="
echo "   Schema Registry Diagnostic Tool"
echo "========================================="

# 1. Check Docker daemon status
echo -e "\n[*] Check 1: Docker Status..."
if ! command -v docker &>/dev/null; then
  echo "[!] Warning: 'docker' CLI not found. Skipping container audits."
else
  CONTAINERS=$(docker ps --filter "name=day12" --format "{{.Names}} (Status: {{.Status}})")
  if [ -z "${CONTAINERS}" ]; then
    echo "[X] Error: No Day 12 containers are running."
  else
    echo "[✓] Active Containers:"
    echo "${CONTAINERS}"
  fi
fi

# 2. Check Port 19092 (Kafka Bootstrap Broker)
echo -e "\n[*] Check 2: Kafka Broker Connectivity (${KAFKA_BROKER})..."
if command -v nc &>/dev/null; then
  if nc -z -w3 localhost 19092; then
    echo "[✓] Port 19092 is open and listening."
  else
    echo "[X] Port 19092 is CLOSED. Kafka broker might be offline."
  fi
else
  # Fallback to python socket check if nc is missing
  python -c "import socket; s = socket.socket(); s.settimeout(2); exit(0 if s.connect_ex(('localhost', 19092)) == 0 else 1)" && \
    echo "[✓] Port 19092 is open (validated via Python)." || \
    echo "[X] Port 19092 is CLOSED. Kafka broker might be offline."
fi

# 3. Check Port 8081 (Schema Registry HTTP API)
echo -e "\n[*] Check 3: Schema Registry Connection (${REGISTRY_URL})..."
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${REGISTRY_URL}/subjects" || echo "000")

if [ "${HTTP_STATUS}" = "200" ]; then
  echo "[✓] Schema Registry REST API is healthy (returned 200)."
elif [ "${HTTP_STATUS}" = "000" ]; then
  echo "[X] Schema Registry port 8081 is closed or unreachable."
else
  echo "[!] Schema Registry returned unexpected HTTP status: ${HTTP_STATUS}"
fi

# 4. Check Subject Registrations and Schema Health
if [ "${HTTP_STATUS}" = "200" ]; then
  echo -e "\n[*] Check 4: Querying subjects list..."
  SUBJECTS=$(curl -s "${REGISTRY_URL}/subjects")
  echo "Subjects: ${SUBJECTS}"
  
  if [ "${SUBJECTS}" != "[]" ]; then
    echo "Inspecting default compatibility config..."
    GLOBAL_CFG=$(curl -s "${REGISTRY_URL}/config")
    echo "Global Config: ${GLOBAL_CFG}"
  fi
fi

echo -e "\n========================================="
echo "        Diagnostics Check Complete"
echo "========================================="
