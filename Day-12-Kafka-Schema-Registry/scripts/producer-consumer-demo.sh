#!/usr/bin/env bash
# producer-consumer-demo.sh
# End-to-end verification showing Schema Registry-backed Avro schema evolution.

set -euo pipefail

TOPIC="day-12-users"
BOOTSTRAP_SERVERS="localhost:19092"
SCHEMA_REGISTRY="http://localhost:8081"

echo "=== Schema-Aware Producer & Consumer Integration Demo ==="

# 1. Verify environment
if ! command -v python &>/dev/null; then
  echo "[X] Error: Python 3 is required to run this integration demo."
  exit 1
fi

# Create python virtual environment if not present
if [ ! -d "venv-day12" ]; then
  echo "[*] Creating Python Virtual Environment (venv-day12)..."
  python -m venv venv-day12
fi

# Activate virtual environment
# Windows vs Unix activate script check
if [ -f "venv-day12/Scripts/activate" ]; then
  source venv-day12/Scripts/activate
else
  source venv-day12/bin/activate
fi

echo "[*] Installing Python dependencies..."
pip install -r ../producer/requirements.txt --quiet

# 2. Reset Schema Registry compatibility (set to BACKWARD for testing)
echo -e "\n[*] Resetting compatibility level of '${TOPIC}-value' to BACKWARD..."
curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "BACKWARD"}' \
  "${SCHEMA_REGISTRY}/config/${TOPIC}-value" || true

# 3. Produce events using v1 Schema
echo -e "\n[1] Producing events with v1 schema..."
python ../producer/producer.py --schema ../schemas/user-v1.avsc --topic "${TOPIC}" --count 3

# 4. Consume events (v1)
echo -e "\n[2] Consuming events with client-side deserialization..."
python ../consumer/consumer.py --topic "${TOPIC}" --timeout 5

# 5. Produce events using evolved compatible v2 Schema
echo -e "\n[3] Producing events with evolved compatible v2 schema..."
python ../producer/producer.py --schema ../schemas/user-v2-compatible.avsc --topic "${TOPIC}" --count 3

# 6. Consume events again
# The consumer should consume both v1 and v2 events seamlessly, deserializing them
echo -e "\n[4] Consuming all events from the beginning (v1 + v2)..."
python ../consumer/consumer.py --topic "${TOPIC}" --group-id "avro-group-new-$(date +%s)" --timeout 5

# 7. Test Incompatible Schema
echo -e "\n[5] Attempting to produce with INCOMPATIBLE schema..."
echo "[*] (This should fail at schema registration time because we deleted a required field and added a field without defaults)"
if python ../producer/producer.py --schema ../schemas/user-v2-incompatible.avsc --topic "${TOPIC}" --count 1 2>&1 | grep -E -i "conflict|incompatible|409|422"; then
  echo -e "\n[✓] SUCCESS: Schema Registry successfully BLOCKED the incompatible schema!"
else
  echo -e "\n[X] FAILURE: Schema Registry allowed the incompatible schema registration! Check configuration."
  exit 1
fi

echo -e "\n=== End-to-End Demo Finished Successfully ==="
deactivate
