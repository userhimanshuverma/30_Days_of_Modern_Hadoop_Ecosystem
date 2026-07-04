#!/usr/bin/env bash
# verify-producer.sh — Day 13 Ingestion Pipeline Verification
# Verifies producer configuration and connectivity to Kafka

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG_FILE="$PARENT_DIR/configs/producer_config.json"

echo "=== [Verification: Producer Client Configuration & Connectivity] ==="

# 1. Check Python installation
if ! command -v python3 &>/dev/null; then
    echo "[X] Error: python3 is not installed or not in PATH."
    exit 1
fi
echo "[✓] Python 3 is installed: $(python3 --version)"

# 2. Check if configuration file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "[X] Error: Producer config file not found at $CONFIG_FILE"
    exit 1
fi
echo "[✓] Producer config file found: $CONFIG_FILE"

# 3. Check Python dependencies
echo "[*] Verifying python client dependencies..."
if ! python3 -c "import confluent_kafka" &>/dev/null; then
    echo "[X] Error: 'confluent-kafka' Python module is not installed."
    echo "    Please run: pip install -r $PARENT_DIR/producer/requirements.txt"
    exit 1
fi
echo "[✓] Python module 'confluent-kafka' is available."

# 4. Extract bootstrap server and verify TCP connectivity
BOOTSTRAP_SERVER=$(python3 -c "import json; print(json.load(open('$CONFIG_FILE'))['bootstrap.servers'])")
echo "[*] Target Kafka Bootstrap Broker: $BOOTSTRAP_SERVER"

HOST=$(echo "$BOOTSTRAP_SERVER" | cut -d: -f1)
PORT=$(echo "$BOOTSTRAP_SERVER" | cut -d: -f2)

# If host is localhost, resolve to 127.0.0.1
if [ "$HOST" = "localhost" ]; then
    HOST="127.0.0.1"
fi

echo "[*] Testing socket connection to broker at $HOST:$PORT..."
if command -v nc &>/dev/null; then
    if nc -z -w 5 "$HOST" "$PORT"; then
        echo "[✓] Kafka broker port is open and accessible!"
    else
        echo "[X] Error: Cannot connect to Kafka broker at $HOST:$PORT. Check if Docker is running."
        exit 1
    fi
elif command -v timeout &>/dev/null && bash -c "cat < /dev/null > /dev/tcp/$HOST/$PORT" &>/dev/null; then
    echo "[✓] Kafka broker port is open and accessible!"
else
    # Fallback check using python socket
    if python3 -c "import socket; s = socket.socket(); s.settimeout(5); s.connect(('$HOST', int('$PORT'))); s.close()" &>/dev/null; then
        echo "[✓] Kafka broker port is open and accessible!"
    else
        echo "[X] Error: Cannot connect to Kafka broker at $HOST:$PORT. Check if Docker is running."
        exit 1
    fi
fi

echo "[✓] Producer verification complete. Broker is reachable and client libraries are present."
exit 0
