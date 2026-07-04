#!/usr/bin/env bash
# verify-storage.sh — Day 13 Ingestion Pipeline Verification
# Verifies files stored in MinIO storage bucket.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG_FILE="$PARENT_DIR/configs/consumer_config.json"

echo "=== [Verification: Storage Layer (MinIO)] ==="

# Check config file
if [ ! -f "$CONFIG_FILE" ]; then
    echo "[X] Error: Config file not found at $CONFIG_FILE"
    exit 1
fi

# Run inline python to check S3 bucket
python3 -c "
import json
import sys
import boto3
from botocore.client import Config

# Load config
try:
    with open('$CONFIG_FILE') as f:
        conf = json.load(f)
except Exception as e:
    print(f'[X] Error loading config: {e}')
    sys.exit(1)

endpoint = conf.get('s3.endpoint', 'http://localhost:9000')
bucket_name = conf.get('s3.bucket', 'clickstream-lake')
access_key = conf.get('s3.access.key', 'minioadmin')
secret_key = conf.get('s3.secret.key', 'minioadmin')

print(f'[*] Connecting to MinIO at {endpoint}...')
try:
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    # List objects
    response = s3.list_objects_v2(Bucket=bucket_name)
    
    if 'Contents' not in response or len(response['Contents']) == 0:
        print('[!] Storage Bucket is empty. No files ingested yet.')
        sys.exit(0)
        
    print(f'[✓] Found {len(response[\"Contents\"])} objects in bucket \"{bucket_name}\":\n')
    
    # Header
    print(f'{ \"Partition / Object Key\":<70} | {\"Size (Bytes)\":<12}')
    print('-' * 85)
    
    total_size = 0
    for obj in response['Contents']:
        print(f'{obj[\"Key\"]:<70} | {obj[\"Size\"]:>12,}')
        total_size += obj['Size']
        
    print('-' * 85)
    print(f'Total Storage Size: {total_size:,} bytes')
    
except Exception as e:
    print(f'[X] Failed to query MinIO bucket \"{bucket_name}\": {e}')
    sys.exit(1)
"
exit 0
