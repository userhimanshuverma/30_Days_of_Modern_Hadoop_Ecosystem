# Ingestion Pipeline Diagrams

This directory contains architectural and data flow diagrams representing our end-to-end data ingestion pipeline.

All diagrams are written in Mermaid markdown syntax, allowing them to render natively in GitHub and markdown readers.

## List of Diagrams in README:
1.  **End-to-End Ingestion Architecture**: Conceptual overview of web application clients routing events down to BI/Analytics engines.
2.  **Application ➔ Kafka ➔ Storage Flow**: Component-level mapping of data payloads.
3.  **Producer Client Workflow**: Internal client thread execution (Buffer, Accumulator, Sender thread, Callbacks).
4.  **Storage Write Flow**: Buffering, parquet conversion, S3 partitioning, and manual commit loops.
5.  **Event Lifecycle**: Stages of a log event from generation to persistence and consumption.
6.  **Data Lake Partition Directory Topology**: Directory hierarchy within the object store.
7.  **Docker Compose Infrastructure Services**: Networked service dependencies.
8.  **Failure Recovery & Durability**: Behavior of clients when a broker node crashes.
9.  **Pipeline Observability & Monitoring**: Prometheus metric collection and dashboard points.
10. **Local multi-node cluster topology**: KRaft broker controller voting setup.
