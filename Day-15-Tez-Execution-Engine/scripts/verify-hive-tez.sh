#!/bin/bash
# verify-hive-tez.sh - Verifies that Apache Hive is configured to execute queries on Apache Tez.

set -e

HIVE_HOST=${1:-"localhost"}
HIVE_PORT=${2:-"10000"}

echo "=== 🔍 STEP 1: Probing HiveServer2 port availability ($HIVE_HOST:$HIVE_PORT) ==="
timeout 15 bash -c "until </dev/tcp/$HIVE_HOST/$HIVE_PORT; do sleep 1; done" 2>/dev/null || {
    echo "❌ Error: HiveServer2 is not accepting connections on $HIVE_HOST:$HIVE_PORT. Ensure container hiveserver2 is healthy."
    exit 1
}
echo "✅ Success: HiveServer2 port is listening."

echo "=== 🔍 STEP 2: Executing engine verification query via Beeline ==="
ENGINE_OUTPUT=$(beeline -u "jdbc:hive2://$HIVE_HOST:$HIVE_PORT/default" -n root -p "" -d org.apache.hive.jdbc.HiveDriver -e "SET hive.execution.engine;" 2>/dev/null | grep "hive.execution.engine=")

echo "Hive returned: $ENGINE_OUTPUT"

if echo "$ENGINE_OUTPUT" | grep -q "tez"; then
    echo "✅ Success: Apache Tez is the active execution engine for Hive!"
else
    echo "❌ Error: Hive execution engine is NOT set to 'tez'. Active configuration is: $ENGINE_OUTPUT"
    exit 1
fi

echo "=== 🎉 Hive-on-Tez Verification Succeeded! ==="
