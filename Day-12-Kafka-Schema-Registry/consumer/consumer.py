#!/usr/bin/env python3
"""
Production-Grade Avro Consumer
Day 12 — Schema Registry
"""

import argparse
import sys
import time
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


def dict_to_user(obj, ctx):
    """Pass-through converter for dictionaries."""
    return obj


def main():
    parser = argparse.ArgumentParser(description="Confluent Schema Registry Avro Consumer")
    parser.add_argument("--topic", default="day-12-users", help="Kafka topic name to read from")
    parser.add_argument("--bootstrap-servers", default="localhost:19092", help="Kafka bootstrap brokers")
    parser.add_argument("--schema-registry", default="http://localhost:8081", help="Schema Registry REST URL")
    parser.add_argument("--group-id", default="avro-user-consumer-group", help="Consumer group ID")
    parser.add_argument("--timeout", type=int, default=15, help="Seconds to wait before shutting down if idle")

    args = parser.parse_args()

    print("=== Starting Schema-Aware Avro Consumer ===")
    print(f"[*] Bootstrap Servers: {args.bootstrap_servers}")
    print(f"[*] Schema Registry:   {args.schema_registry}")
    print(f"[*] Group ID:          {args.group_id}")
    print(f"[*] Subscribed Topic:  {args.topic}")

    # 1. Configure Schema Registry Client
    sr_conf = {'url': args.schema_registry}
    try:
        sr_client = SchemaRegistryClient(sr_conf)
    except Exception as e:
        print(f"[X] Failed to create Schema Registry Client: {e}", file=sys.stderr)
        sys.exit(1)

    # 2. Define Avro Deserializer
    # The Deserializer automatically downloads the writer schema from Schema Registry 
    # based on the Schema ID prepended to the message payload, then parses the record.
    try:
        avro_deserializer = AvroDeserializer(
            schema_registry_client=sr_client,
            from_dict=dict_to_user
        )
    except Exception as e:
        print(f"[X] Failed to initialize Avro Deserializer: {e}", file=sys.stderr)
        sys.exit(1)

    # 3. Configure Deserializing Consumer
    consumer_conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'value.deserializer': avro_deserializer,
        'group.id': args.group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False  # Let's commit manually for safety
    }

    try:
        consumer = DeserializingConsumer(consumer_conf)
        consumer.subscribe([args.topic])
    except Exception as e:
        print(f"[X] Failed to build Kafka Consumer: {e}", file=sys.stderr)
        sys.exit(1)

    print("[*] Consumer active. Waiting for messages. Press Ctrl+C to exit...")
    
    last_message_time = time.time()
    try:
        while True:
            # Poll for messages
            msg = consumer.poll(1.0)
            
            if msg is None:
                # Idle timeout check
                if time.time() - last_message_time > args.timeout:
                    print(f"[*] No messages received for {args.timeout} seconds. Shutting down...")
                    break
                continue

            last_message_time = time.time()

            if msg.error():
                print(f"[-] Consumer error: {msg.error()}", file=sys.stderr)
                continue

            # Process valid message
            key = msg.key()
            value = msg.value()
            partition = msg.partition()
            offset = msg.offset()

            print(f"[✓] Event Decoded Successfully!")
            print(f"    - Key:       {key}")
            print(f"    - Partition: {partition}")
            print(f"    - Offset:    {offset}")
            print(f"    - Payload:   {value}")
            
            # Commit offsets manually
            consumer.commit(msg, asynchronous=True)

    except KeyboardInterrupt:
        print("\n[*] Stopping consumer...")
    except Exception as e:
        print(f"[X] Fatal Exception: {e}", file=sys.stderr)
    finally:
        # Close consumer connection
        print("[*] Closing consumer connection...")
        consumer.close()
        print("[✓] Consumer closed.")


if __name__ == "__main__":
    main()
