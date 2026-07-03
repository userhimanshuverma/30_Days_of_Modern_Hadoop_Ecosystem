#!/usr/bin/env bash
# verify-avro.sh
# Compares serialized payload sizes of standard JSON vs Apache Avro binary.

set -euo pipefail

echo "=== Avro vs JSON Serialization Size Comparison ==="

# Check if Python is available
if ! command -v python &>/dev/null; then
  echo "[X] Error: Python is required to run the comparison."
  exit 1
fi

# Run python inline script to compare sizes
python - << 'EOF'
import json
import io
import sys

# Attempt to import fastavro; if missing, install it or warn user
try:
    import fastavro
except ImportError:
    print("[*] Installing fastavro dependency for comparison...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "fastavro", "--quiet"])
    import fastavro

# 1. Define schema
schema = {
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}
parsed_schema = fastavro.parse_schema(schema)

# 2. Define event payload
record = {
  "id": "usr_101",
  "name": "Alice Developer",
  "email": "alice.developer@enterprise.bank",
  "timestamp": 1719999999000
}

# 3. Serialize as JSON
json_payload = json.dumps(record)
json_bytes = json_payload.encode('utf-8')

# 4. Serialize as Avro (Binary, schemaless writer)
out = io.BytesIO()
fastavro.schemaless_writer(out, parsed_schema, record)
avro_bytes = out.getvalue()

# 5. Output comparison results
print(f"[*] Original Payload Record: {record}\n")
print(f"[1] JSON Format:")
print(f"    - Payload: {json_payload}")
print(f"    - Size:    {len(json_bytes)} bytes")
print(f"[2] Avro Binary Format:")
print(f"    - Payload: {avro_bytes.hex()}")
print(f"    - Size:    {len(avro_bytes)} bytes")
print(f"\n[✓] Space Savings:")
savings = len(json_bytes) - len(avro_bytes)
percentage = (savings / len(json_bytes)) * 100
print(f"    - Avro saved {savings} bytes ({percentage:.2f}% reduction) compared to JSON!")
print(f"    - Note: In addition, Confluent Schema Registry adds a tiny 5-byte header prefix to each message.")
EOF

echo "=== Comparison Complete ==="
