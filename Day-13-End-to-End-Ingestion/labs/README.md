# Day 13 Lab — Building a Production-Grade Ingestion Pipeline

This lab guides you step-by-step through setting up, deploying, and operating a real-time event-driven data ingestion pipeline.

## Lab Execution Roadmap

1.  **Deploy Environment**: Use docker-compose to launch Kafka and MinIO.
2.  **Verify Services**: Check health endpoints and connectivity.
3.  **Run Producer**: Generate mock e-commerce clickstream events.
4.  **Launch Storage Writer**: Run the consumer in the background to buffer data and persist as Parquet to MinIO.
5.  **Simulate Network/Broker Failure**: Kill a broker partition container and check retry states.
6.  **Verify Data Landing**: Check S3 folders and perform checksum counts.
7.  **Clean Environment**: Shut down services and delete persistent volumes.

The full details of these exercises are embedded in the main lesson [README.md](../README.md#section-7--hands-on-lab).
