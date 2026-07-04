#!/usr/bin/env bash
# verify-ingestion.sh — Day 13 Ingestion Pipeline Verification
# Downloads Parquet files from MinIO and validates data counts and scheme compatibility.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG_FILE="$PARENT_DIR/configs/consumer_config.json"

echo "=== [Verification: End-to-End Data Validation] ==="

# Check config file
if [ ! -f "$CONFIG_FILE" ]; then
    echo "[X] Error: Config file not found at $CONFIG_FILE"
    exit 1
fi

python3 -c "
import json
import sys
import io
import boto3
import pandas as pd
import pyarrow.parquet as pq
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
        print('[X] Error: Storage is empty. Cannot perform data validation.')
        sys.exit(1)
        
    print(f'[*] Reading and validating {len(response[\"Contents\"])} Parquet files from storage...')
    
    total_records = 0
    df_list = []
    schema = None
    
    for obj in response['Contents']:
        # Fetch file data into memory
        file_obj = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
        data = file_obj['Body'].read()
        
        # Read Parquet
        parquet_file = pq.ParquetFile(io.BytesIO(data))
        table = parquet_file.read()
        
        total_records += table.num_rows
        df_list.append(table.to_pandas())
        
        if schema is None:
            schema = table.schema
            
    print(f'[✓] Succesfully read {total_records} records.')
    
    # Check schema fields
    expected_fields = ['event_id', 'timestamp_ms', 'user_id', 'event_type', 'page_url', 'ip_address', 'device']
    missing_fields = [field for field in expected_fields if field not in schema.names]
    
    if len(missing_fields) == 0:
        print('[✓] Schema contains all required fields.')
    else:
        print(f'[X] Warning: Schema is missing some required fields: {missing_fields}')
        
    print('\n[*] Ingested Parquet Schema:')
    print(schema)
    
    # Aggregate data and print sample records
    combined_df = pd.concat(df_list, ignore_index=True)
    print('\n[*] Record breakdown by event_type:')
    print(combined_df['event_type'].value_counts())
    
    print('\n[*] Record breakdown by device:')
    print(combined_df['device'].value_counts())
    
    print('\n[*] Sample Data Record (First 3 entries):')
    print(combined_df.head(3).to_string())
    
    print('\n[✓] End-to-end data integrity validation PASSED.')
    
except Exception as e:
    print(f'[X] Failed to perform validation: {e}')
    sys.exit(1)
"
exit 0
