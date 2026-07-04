#!/usr/bin/env python3
"""
Production-Grade Stream Consumer & Storage Writer
Day 13 — End-to-End Data Ingestion Pipeline
"""

import io
import json
import os
import sys
import time
import uuid
import signal
import argparse
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config
from confluent_kafka import Consumer, KafkaError, TopicPartition


class StorageWriterConsumer:
    def __init__(self, config_path, topic_override=None):
        # 1. Load Configurations
        self.config = self.load_config(config_path)
        
        # Extract Kafka options
        self.kafka_conf = {
            'bootstrap.servers': self.config.get('bootstrap.servers', 'localhost:9092'),
            'group.id': self.config.get('group.id', 'clickstream-storage-writer-group'),
            'auto.offset.reset': self.config.get('auto.offset.reset', 'earliest'),
            'enable.auto.commit': False  # CRITICAL: We manage offsets manually
        }
        
        # Extract Storage options
        self.s3_endpoint = self.config.get('s3.endpoint', 'http://localhost:9000')
        self.s3_access_key = self.config.get('s3.access.key', 'minioadmin')
        self.s3_secret_key = self.config.get('s3.secret.key', 'minioadmin')
        self.s3_bucket = self.config.get('s3.bucket', 'clickstream-lake')
        
        # Extract Buffer settings
        self.buffer_size_limit = self.config.get('buffer.size.records', 1000)
        self.buffer_timeout_limit = self.config.get('buffer.timeout.seconds', 10)
        
        # Topic
        self.topic = topic_override or self.config.get('topic', 'clickstream-events')
        
        # State variables
        self.buffer = []
        self.last_flush_time = time.time()
        self.running = True
        self.consumer = None
        self.s3_client = None
        
        # Setup signals
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        
    def load_config(self, config_path):
        """Loads configuration from JSON file."""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"[X] Failed to load config from {config_path}: {e}", file=sys.stderr)
            sys.exit(1)
            
    def handle_shutdown(self, signum, frame):
        """Triggers graceful shutdown on OS signals."""
        print(f"\n[-] Shutdown signal ({signum}) received. Initiating graceful shutdown...")
        self.running = False

    def init_clients(self):
        """Initializes Kafka consumer and MinIO (S3) client."""
        # Setup S3 Client
        print(f"[*] Initializing connection to MinIO/S3 endpoint: {self.s3_endpoint}")
        try:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.s3_endpoint,
                aws_access_key_id=self.s3_access_key,
                aws_secret_access_key=self.s3_secret_key,
                config=Config(signature_version='s3v4'),
                region_name='us-east-1' # Dummy region for MinIO
            )
            # Check bucket accessibility
            self.s3_client.head_bucket(Bucket=self.s3_bucket)
            print(f"[✓] Successfully verified connection to storage bucket: {self.s3_bucket}")
        except Exception as e:
            print(f"[X] Failed to connect to MinIO/S3 bucket: {e}", file=sys.stderr)
            sys.exit(1)

        # Setup Kafka Consumer
        print(f"[*] Initializing Kafka Consumer group: {self.kafka_conf['group.id']}")
        try:
            self.consumer = Consumer(self.kafka_conf)
            self.consumer.subscribe([self.topic])
            print(f"[✓] Subscribed to topic: {self.topic}")
        except Exception as e:
            print(f"[X] Failed to build Kafka Consumer: {e}", file=sys.stderr)
            sys.exit(1)

    def write_buffer_to_storage(self):
        """Converts buffer of events to Parquet and uploads to S3, then commits offsets."""
        if not self.buffer:
            self.last_flush_time = time.time()
            return
            
        print(f"[*] Flushing buffer of {len(self.buffer)} records to storage...")
        
        # 1. Group records by partition paths (year, month, day, hour)
        partitioned_data = {}
        for record, partition_info in self.buffer:
            # We determine partition from timestamp_ms
            ts_sec = record.get("timestamp_ms", int(time.time() * 1000)) / 1000.0
            dt = datetime.utcfromtimestamp(ts_sec)
            
            # Format partitions
            partition_path = f"year={dt.strftime('%Y')}/month={dt.strftime('%m')}/day={dt.strftime('%d')}/hour={dt.strftime('%H')}"
            
            if partition_path not in partitioned_data:
                partitioned_data[partition_path] = []
            partitioned_data[partition_path].append((record, partition_info))
            
        # 2. Write one file per partition path to S3
        for partition_path, records_with_meta in partitioned_data.items():
            records = [r[0] for r in records_with_meta]
            metadata = [r[1] for r in records_with_meta]
            
            # Convert to Pandas DataFrame
            df = pd.DataFrame(records)
            
            # Convert to PyArrow Table
            table = pa.Table.from_pandas(df)
            
            # Serialize to Parquet byte stream
            buffer_io = io.BytesIO()
            pq.write_table(table, buffer_io, compression='SNAPPY')
            buffer_io.seek(0)
            
            # Generate unique file name
            filename = f"clickstream_{int(time.time())}_{uuid.uuid4().hex[:8]}.parquet"
            s3_key = f"{partition_path}/{filename}"
            
            # Upload to MinIO/S3
            print(f"    -> Uploading {len(records)} records to {s3_key}...")
            try:
                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=buffer_io.getvalue()
                )
            except Exception as e:
                print(f"[X] CRITICAL: Storage write failed: {e}", file=sys.stderr)
                print("[!] Stopping offset commit. Consumer will retry on restart.", file=sys.stderr)
                raise e

        # 3. Manually commit offsets up to the last processed message of each partition in the buffer
        # Collect the highest offset for each partition in this flush batch
        highest_offsets = {}
        for _, (topic, partition, offset) in self.buffer:
            key = (topic, partition)
            if key not in highest_offsets or offset > highest_offsets[key]:
                highest_offsets[key] = offset
                
        # Build TopicPartition list to commit (commit next offset = current + 1)
        partitions_to_commit = [
            TopicPartition(topic, partition, offset + 1)
            for (topic, partition), offset in highest_offsets.items()
        ]
        
        print(f"[*] Committing offsets to Kafka: {[(tp.partition, tp.offset) for tp in partitions_to_commit]}")
        try:
            self.consumer.commit(offsets=partitions_to_commit, asynchronous=False)
            print("[✓] Offsets committed successfully.")
        except Exception as e:
            print(f"[!] Warning: Offset commit failed: {e}", file=sys.stderr)
            # In production, we might want to alert, but since data is already in S3,
            # this might lead to duplicate processing next time. We continue.
            
        # 4. Clear buffer and update flush timestamp
        self.buffer.clear()
        self.last_flush_time = time.time()
        print("[✓] Buffer flush cycle complete.")

    def run(self):
        """Core consumer loop."""
        self.init_clients()
        
        print("\n=== Clickstream Consumer & Storage Writer Running ===")
        print("[*] Press Ctrl+C to stop gracefully.")
        
        poll_timeout = 1.0 # Poll every 1 second
        
        try:
            while self.running:
                # Poll Kafka Broker
                msg = self.consumer.poll(poll_timeout)
                
                if msg is None:
                    # Check if we should flush based on timeout
                    elapsed = time.time() - self.last_flush_time
                    if len(self.buffer) > 0 and elapsed >= self.buffer_timeout_limit:
                        print(f"[*] Buffer timeout ({elapsed:.1f}s >= {self.buffer_timeout_limit}s) reached.")
                        self.write_buffer_to_storage()
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event (not a real error)
                        continue
                    else:
                        print(f"[X] Kafka error: {msg.error()}", file=sys.stderr)
                        time.sleep(2)  # Backoff
                        continue

                # Parse message value (JSON clickstream payload)
                try:
                    payload = json.loads(msg.value().decode('utf-8'))
                    partition_info = (msg.topic(), msg.partition(), msg.offset())
                    self.buffer.append((payload, partition_info))
                except Exception as e:
                    print(f"[!] Failed to parse message value at partition {msg.partition()} offset {msg.offset()}: {e}", file=sys.stderr)
                    # Poison pill: Skip or commit offset. In production, send to Dead Letter Queue (DLQ).
                    # For simplicity, we just skip it, but in our case we commit its offset
                    self.consumer.commit(message=msg, asynchronous=False)
                    continue

                # Check if buffer size limit is reached
                if len(self.buffer) >= self.buffer_size_limit:
                    print(f"[*] Buffer size limit ({len(self.buffer)} >= {self.buffer_size_limit}) reached.")
                    self.write_buffer_to_storage()
                    
                # Periodic logs
                if len(self.buffer) > 0 and len(self.buffer) % 200 == 0:
                    print(f"[*] Buffer count: {len(self.buffer)}/{self.buffer_size_limit} (Last flush: {time.time() - self.last_flush_time:.1f}s ago)")

            # Loop finished (graceful exit)
            if self.buffer:
                print(f"[*] Flushing remaining {len(self.buffer)} messages in buffer before exiting...")
                self.write_buffer_to_storage()
                
        except Exception as e:
            print(f"[X] Consumer crashed due to unhandled exception: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
        finally:
            print("[*] Closing Kafka Consumer...")
            if self.consumer:
                self.consumer.close()
            print("[✓] Shutdown complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Clickstream to MinIO Parquet Ingestor")
    parser.add_argument("--config", default="configs/consumer_config.json", help="Path to consumer configuration JSON file")
    parser.add_argument("--topic", default=None, help="Override Kafka topic to consume from")
    args = parser.parse_args()

    writer = StorageWriterConsumer(args.config, topic_override=args.topic)
    writer.run()
