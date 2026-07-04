#!/usr/bin/env python3
"""
Production-Grade Clickstream Event Producer
Day 13 — End-to-End Data Ingestion Pipeline
"""

import json
import random
import sys
import time
import uuid
import argparse
from datetime import datetime
from confluent_kafka import Producer


# Event templates for generation
PAGES = [
    "/home", "/products", "/products/electronics", "/products/apparel",
    "/cart", "/checkout", "/payment", "/order-confirmation"
]
EVENT_TYPES = ["view", "click", "add_to_cart", "remove_from_cart", "purchase"]
DEVICES = ["mobile-ios", "mobile-android", "desktop-chrome", "desktop-firefox", "tablet-safari"]
USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15",
    "Mozilla/5.0 (Linux; Android 13; SM-S901B) AppleWebKit/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (iPad; CPU OS 16_5 like Mac OS X) AppleWebKit/605.1.15"
]

def delivery_report(err, msg):
    """Callback triggered on message delivery status."""
    if err is not None:
        print(f"[-] Message delivery failed: {err}", file=sys.stderr)
    else:
        # Avoid flooding the console by printing every message, but we print some or write to stdout
        pass

def load_config(config_path):
    """Loads configuration properties from a JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"[X] Failed to load config from {config_path}: {e}", file=sys.stderr)
        sys.exit(1)

def generate_clickstream_event(user_id):
    """Generates a structured clickstream event."""
    event_type = random.choice(EVENT_TYPES)
    page_url = random.choice(PAGES)
    device = random.choice(DEVICES)
    user_agent = random.choice(USER_AGENTS)
    ip_address = f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
    
    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp_ms": int(time.time() * 1000),
        "user_id": user_id,
        "event_type": event_type,
        "page_url": page_url,
        "ip_address": ip_address,
        "user_agent": user_agent,
        "device": device
    }
    return event

def main():
    parser = argparse.ArgumentParser(description="Production Clickstream Event Producer")
    parser.add_argument("--config", default="configs/producer_config.json", help="Path to producer configuration JSON file")
    parser.add_argument("--topic", default="clickstream-events", help="Kafka topic to write events to")
    parser.add_argument("--count", type=int, default=1000, help="Number of events to generate")
    parser.add_argument("--delay", type=float, default=0.01, help="Delay in seconds between event emission")
    
    args = parser.parse_args()
    
    print("=== Starting Clickstream Producer ===")
    print(f"[*] Loading config:  {args.config}")
    print(f"[*] Target Topic:    {args.topic}")
    print(f"[*] Total Events:    {args.count}")
    print(f"[*] Emission Delay:  {args.delay}s")

    # Load configuration
    config = load_config(args.config)
    
    # Initialize Producer
    try:
        producer = Producer(config)
    except Exception as e:
        print(f"[X] Failed to build Kafka Producer: {e}", file=sys.stderr)
        sys.exit(1)
        
    # Generate user pool
    user_pool = [f"usr-{random.randint(10000, 99999)}" for _ in range(500)]
    
    success_count = 0
    start_time = time.time()
    
    try:
        for idx in range(1, args.count + 1):
            user_id = random.choice(user_pool)
            event = generate_clickstream_event(user_id)
            
            # Serialize key (user_id) and value (JSON string)
            key = user_id.encode('utf-8')
            value = json.dumps(event).encode('utf-8')
            
            # Asynchronously send message
            # The client library buffers internally and batches according to configuration
            producer.produce(
                topic=args.topic,
                key=key,
                value=value,
                on_delivery=delivery_report
            )
            
            success_count += 1
            if idx % 100 == 0:
                print(f"[✓] Queued {idx}/{args.count} events to topic: {args.topic}")
                # Serve delivery queue callbacks to clear memory
                producer.poll(0)
                
            if args.delay > 0:
                time.sleep(args.delay)
                
    except KeyboardInterrupt:
        print("\n[-] Producer execution interrupted by user.")
    finally:
        # Flush message queue to guarantee delivery before exit
        print("[*] Flushing producer queue (waiting for broker acknowledgements)...")
        undelivered = producer.flush(timeout=10.0)
        if undelivered > 0:
            print(f"[!] Warning: {undelivered} messages could not be delivered to broker.", file=sys.stderr)
        else:
            print("[✓] All messages successfully delivered!")
            
    elapsed = time.time() - start_time
    print(f"\n=== Execution Summary ===")
    print(f"[*] Total events queued and sent: {success_count}")
    print(f"[*] Elapsed time: {elapsed:.2f} seconds")
    print(f"[*] Throughput:   {success_count / elapsed:.2f} events/sec")

if __name__ == "__main__":
    main()
