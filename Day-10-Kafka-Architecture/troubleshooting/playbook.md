# Day 10 Troubleshooting Playbook — Apache Kafka Operations

This playbook is a quick-reference runbook for operational engineers diagnosing and resolving anomalies in the Kafka cluster.

---

## 🔍 Diagnostic Toolkit (Cheat Sheet)

All commands are run using scripts packaged inside the Kafka brokers. If running from a host machine, execute these inside a running container using `docker exec -it kafka1-day10 <command>`.

### 1. Broker & Cluster Status Check
```bash
# Verify container states
docker ps -f name=kafka

# Query KRaft Quorum Consensus Status (Metadata replication lag)
docker exec -it kafka1-day10 kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status

# Describe controller replicas log offsets
docker exec -it kafka1-day10 kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication
```

### 2. Topic & Partition Inspection
```bash
# List all topics
docker exec -it kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --list

# Show details of a specific topic
docker exec -it kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic my-topic

# Filter for under-replicated partitions (ISR size < Replicas size)
docker exec -it kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions

# Filter for offline partitions (partition has no leader)
docker exec -it kafka1-day10 kafka-topics.sh --bootstrap-server localhost:9092 --describe --unavailable-partitions
```

### 3. Consumer Group Diagnostics
```bash
# List active consumer groups
docker exec -it kafka1-day10 kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Describe group details, partition assignments, and lag
docker exec -it kafka1-day10 kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-consumer-group

# Show state and members of a group
docker exec -it kafka1-day10 kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-consumer-group --state

# Reset consumer group offsets to the beginning for a topic
docker exec -it kafka1-day10 kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-consumer-group --reset-offsets --to-earliest --topic my-topic --execute
```

---

## 🛠️ Common Incidents & Remediation

### Incident 1: Under-Replicated Partitions (URPs)
* **Symptoms**: `verify-replication.sh` prints warnings. Monitoring alerts fire for the `UnderReplicatedPartitions` JMX metric.
* **Root Cause**: One or more brokers have crashed, run out of disk space, or have lost network connectivity to the partition leader, preventing them from fetching log segments.
* **Resolution**:
  1. Find URPs: `kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions`.
  2. Identify the lagging replica node IDs (the numbers in the `Replicas` list that are missing from `Isr`).
  3. Verify the target container's process state: `docker ps -a` or check raw JVM metrics on the target host.
  4. Review target broker logs for out-of-memory (OOM) errors, disk failures, or network partitions.
  5. Restart the broker. Once online, the broker will automatically fetch offsets from the leader and catch up.

### Incident 2: High Consumer Lag
* **Symptoms**: Downstream consumers are processing stale data; messages are not showing up in real time.
* **Root Cause**: The consumer application's processing speed is slower than the producer ingestion rate, or the consumer has crashed, causing a rebalance loop.
* **Resolution**:
  1. Check lag: `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-consumer-group`.
  2. Identify if lag is isolated to a single partition or spread across all partitions.
  3. If isolated to one partition, check for a stalled consumer thread or high data skew on that partition key.
  4. If spread across all, increase consumer parallelism by adding more consumer instances (up to the number of partitions).
  5. Optimize consumer performance (increase `max.poll.records`, optimize database writes, or run asynchronous handlers).

### Incident 3: Leader Election Failures / Offline Partitions
* **Symptoms**: Producers fail with `NotLeaderOrFollowerException` or `TimeoutException`. Consuming halts.
* **Root Cause**: All brokers holding replicas of a partition have crashed or are out-of-sync, leaving no eligible node to become the leader.
* **Resolution**:
  1. Find offline partitions: `kafka-topics.sh --bootstrap-server localhost:9092 --describe --unavailable-partitions`.
  2. Boot up the offline replicas.
  3. If replicas cannot be recovered due to hardware failure, you must perform unclean leader election (data loss warning!) by setting `unclean.leader.election.enable=true` dynamically or in the configuration, or force electing an out-of-sync follower.
