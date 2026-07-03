# Docker Cluster Runbook — Day 12

This directory contains the Docker Compose environment required to spin up a single-node Kafka broker (running in KRaft mode) and a Confluent Schema Registry instance.

## 📦 Services & Ports
- **Kafka Broker (`kafka-day12`)**: Exposed on external host port `19092` (internal port `9092`).
- **Schema Registry (`schema-registry-day12`)**: Exposed on port `8081`.
- **AKHQ Kafka Web UI (`akhq-day12`)**: Exposed on port `8082` (visualizes brokers, topics, active consumers, and schemas).

---

## 🚀 Lifecycle Commands

### Start the Cluster
To start all services in the background:
```bash
docker-compose up -d
```

### Check Logs
To monitor container startup logs:
```bash
docker-compose logs -f
```

### Verify Container Health
To inspect container health statuses:
```bash
docker-compose ps
```
*Wait until all services show `(healthy)` before starting verification scripts.*

### Stop and Clean Up
To stop the services and retain data volumes:
```bash
docker-compose down
```

To wipe all data volumes (cold reset):
```bash
docker-compose down -v
```

---

## 🛠️ Validation Commands

### Test Kafka Topic Creation
Verify that Kafka is reachable and create the `day-12-users` topic manually (optional, as the producer will create it automatically if needed):
```bash
docker exec -it kafka-day12 kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic day-12-users --partitions 3 --replication-factor 1
```

### Test Schema Registry REST API
Query the Schema Registry to see if the HTTP daemon is active:
```bash
curl -s http://localhost:8081/subjects
```
*Expected output: `[]` (an empty JSON array indicating no schemas are registered yet).*
