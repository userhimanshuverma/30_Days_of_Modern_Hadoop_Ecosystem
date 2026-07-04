# Apache Kafka Broker & Cluster Configurations

This directory contains details about the Apache Kafka broker and topic configurations used for the Day 13 End-to-End Data Ingestion Pipeline.

## Topic Configuration Properties

For a production-grade ingestion topic like `clickstream-events`, the following parameters are recommended:

*   **Partitions**: `3` (for local development; `30+` in production to scale across brokers).
*   **Replication Factor**: `3` (for production high-availability; `1` for local compose).
*   **min.insync.replicas**: `2` (guarantees that at least two replicas acknowledge a write when `acks=all`).
*   **cleanup.policy**: `delete` (since clickstream events are log data rather than stateful records).
*   **retention.ms**: `604800000` (7 days retention before purging log segments).
*   **compression.type**: `producer` (broker retains the compression algorithm applied by the producer client, avoiding expensive decompression/recompression cycles).
