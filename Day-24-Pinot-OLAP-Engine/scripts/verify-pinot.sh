#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIGS_DIR="$ROOT_DIR/configs"

echo "======================================================================"
echo "🏗️ Verifying Pinot Cluster and Registering Configurations"
echo "======================================================================"

# 1. Verify Pinot components are running
components=("pinot-controller" "pinot-broker" "pinot-server" "pinot-minion")
for comp in "${components[@]}"; do
    if ! docker ps | grep -q "$comp"; then
        echo "❌ Error: $comp container is not running."
        echo "Please start the cluster using: docker compose -f $ROOT_DIR/docker/docker-compose.yml up -d"
        exit 1
    fi
done
echo "✅ All Pinot containers are running."

# 2. Wait for Controller health
echo "Waiting for Pinot Controller to become healthy..."
until curl -s -o /dev/null -w "%{http_code}" http://localhost:9000/health | grep -q "200"; do
    echo -n "."
    sleep 2
done
echo ""
echo "✅ Pinot Controller is healthy and accepting connections on port 9000."

# 3. Wait for Broker health
echo "Waiting for Pinot Broker to become healthy..."
until curl -s -o /dev/null -w "%{http_code}" http://localhost:8099/health | grep -q "200"; do
    echo -n "."
    sleep 2
done
echo ""
echo "✅ Pinot Broker is healthy and accepting connections on port 8099."

# 4. Wait for Server health
echo "Waiting for Pinot Server to become healthy..."
until curl -s -o /dev/null -w "%{http_code}" http://localhost:8098/health | grep -q "200"; do
    echo -n "."
    sleep 2
done
echo ""
echo "✅ Pinot Server is healthy and accepting connections on port 8098."

# 5. Register Schema
echo "Registering schema 'user_registrations'..."
if [ -f "$CONFIGS_DIR/user-registrations-schema.json" ]; then
    TMP_OUT=$(mktemp)
    RESPONSE=$(curl -s -w "%{http_code}" -o "$TMP_OUT" -X POST -F schema=@"$CONFIGS_DIR/user-registrations-schema.json" http://localhost:9000/schemas)
    if [ "$RESPONSE" -eq 200 ] || [ "$RESPONSE" -eq 209 ]; then
        echo "✅ Schema registered successfully (HTTP $RESPONSE)."
    else
        echo "❌ Failed to register schema. HTTP Code: $RESPONSE"
        cat "$TMP_OUT"
        rm -f "$TMP_OUT"
        exit 1
    fi
    rm -f "$TMP_OUT"
else
    echo "❌ Schema file not found: $CONFIGS_DIR/user-registrations-schema.json"
    exit 1
fi

# 6. Register Realtime Table
echo "Registering realtime table 'user_registrations_REALTIME'..."
if [ -f "$CONFIGS_DIR/user-registrations-table-realtime.json" ]; then
    TMP_OUT=$(mktemp)
    RESPONSE=$(curl -s -w "%{http_code}" -o "$TMP_OUT" -X POST -H "Content-Type: application/json" -d @"$CONFIGS_DIR/user-registrations-table-realtime.json" http://localhost:9000/tables)
    if [ "$RESPONSE" -eq 200 ] || [ "$RESPONSE" -eq 209 ]; then
        echo "✅ Realtime table registered successfully (HTTP $RESPONSE)."
    else
        echo "❌ Failed to register realtime table. HTTP Code: $RESPONSE"
        cat "$TMP_OUT"
        rm -f "$TMP_OUT"
        exit 1
    fi
    rm -f "$TMP_OUT"
else
    echo "❌ Realtime table file not found: $CONFIGS_DIR/user-registrations-table-realtime.json"
    exit 1
fi

# 7. Register Offline Table
echo "Registering offline table 'user_registrations_OFFLINE'..."
if [ -f "$CONFIGS_DIR/user-registrations-table-offline.json" ]; then
    TMP_OUT=$(mktemp)
    RESPONSE=$(curl -s -w "%{http_code}" -o "$TMP_OUT" -X POST -H "Content-Type: application/json" -d @"$CONFIGS_DIR/user-registrations-table-offline.json" http://localhost:9000/tables)
    if [ "$RESPONSE" -eq 200 ] || [ "$RESPONSE" -eq 209 ]; then
        echo "✅ Offline table registered successfully (HTTP $RESPONSE)."
    else
        echo "❌ Failed to register offline table. HTTP Code: $RESPONSE"
        cat "$TMP_OUT"
        rm -f "$TMP_OUT"
        exit 1
    fi
    rm -f "$TMP_OUT"
else
    echo "❌ Offline table file not found: $CONFIGS_DIR/user-registrations-table-offline.json"
    exit 1
fi

echo "======================================================================"
echo "🎉 Pinot Configuration and Ingestion Pipeline Registered Successfully!"
echo "======================================================================"
echo "Explore the Pinot Controller Console: http://localhost:9000"
echo "======================================================================"
