#!/usr/bin/env python3
"""
Production-Grade Avro Producer
Day 12 — Schema Registry
"""

import argparse
import sys
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


def delivery_report(err, msg):
    """Callback triggered on message delivery status."""
    if err is not None:
        print(f"[-] Message delivery failed for key {msg.key()}: {err}", file=sys.stderr)
    else:
        print(f"[✓] Message delivered to partition {msg.partition()} at offset {msg.offset()}")


def load_schema(schema_path):
    """Reads the Avro schema file from local disk."""
    try:
        with open(schema_path, 'r') as f:
            return f.read()
    except FileNotFoundError:
        print(f"[X] Error: Schema file not found at {schema_path}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[X] Error reading schema: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Confluent Schema Registry Avro Producer")
    parser.add_argument("--schema", required=True, help="Path to the Avro schema file (.avsc)")
    parser.add_argument("--topic", default="day-12-users", help="Kafka topic name to write to")
    parser.add_argument("--bootstrap-servers", default="localhost:19092", help="Kafka bootstrap brokers")
    parser.add_argument("--schema-registry", default="http://localhost:8081", help="Schema Registry REST URL")
    parser.add_argument("--count", type=int, default=5, help="Number of records to produce")
    
    args = parser.parse_args()

    print("=== Starting Schema-Aware Avro Producer ===")
    print(f"[*] Bootstrap Servers: {args.bootstrap_servers}")
    print(f"[*] Schema Registry:   {args.schema_registry}")
    print(f"[*] Target Topic:      {args.topic}")
    print(f"[*] Reading Schema:    {args.schema}")

    # 1. Load schema from path
    schema_str = load_schema(args.schema)

    # 2. Configure Schema Registry Client
    sr_conf = {'url': args.schema_registry}
    try:
        sr_client = SchemaRegistryClient(sr_conf)
    except Exception as e:
        print(f"[X] Failed to create Schema Registry Client: {e}", file=sys.stderr)
        sys.exit(1)

    # 3. Define Avro Serializer
    # The AvroSerializer handles registering the schema on the fly under the subject: <topic>-value
    try:
        avro_serializer = AvroSerializer(
            schema_registry_client=sr_client,
            schema_str=schema_str,
            to_dict=lambda obj, ctx: obj  # Pass direct dictionary
        )
    except Exception as e:
        print(f"[X] Failed to initialize Avro Serializer: {e}", file=sys.stderr)
        sys.exit(1)

    # 4. Configure Serializing Producer
    producer_conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'value.serializer': avro_serializer,
        'acks': 'all',
        'enable.idempotence': True,
        'retries': 5
    }

    try:
        producer = SerializingProducer(producer_conf)
    except Exception as e:
        print(f"[X] Failed to build Kafka Producer: {e}", file=sys.stderr)
        sys.exit(1)

    # 5. Generate and Send Sample Data
    # Let's craft data that fits standard schema fields
    timestamp_ms = int(time.time() * 1000)
    
    # We will check the schema content to determine which fields to send
    # This keeps our producer dynamic for v1 vs v2
    has_phone = "phoneNumber" in schema_str
    has_status = "status" in schema_str
    has_age = '"name": "age"' in schema_str

    print(f"[*] Generating {args.count} test messages...")
    for i in range(1, args.count + 1):
        user_id = f"usr_{100 + i}"
        
        # Base record matching v1
        record = {
            "id": user_id,
            "name": f"User Name {i}",
            "email": f"user{i}@example.com",
            "timestamp": timestamp_ms
        }
        
        # Add compatible v2 fields if they exist in schema
        if has_phone:
            record["phoneNumber"] = f"+1-555-010{i}"
        if has_status:
            record["status"] = "ACTIVE"
            
        # Add incompatible fields if they exist in schema
        if has_age:
            record["age"] = 20 + i
            # Incompatible schema v2 removed 'email', so remove it from payload
            if "email" in record:
                del record["email"]

        print(f"[*] Producing record: {record}")
        try:
            producer.produce(
                topic=args.topic,
                key=user_id,
                value=record,
                on_delivery=delivery_report
            )
        except Exception as e:
            print(f"[X] Error during production: {e}", file=sys.stderr)
            # Flush existing messages and exit
            producer.flush()
            sys.exit(1)
        
        # Poll to trigger delivery callbacks
        producer.poll(0)
        time.sleep(0.5)

    print("[*] Flushing producer queue...")
    producer.flush()
    print("[✓] All messages processed.")


if __name__ == "__main__":
    main()
