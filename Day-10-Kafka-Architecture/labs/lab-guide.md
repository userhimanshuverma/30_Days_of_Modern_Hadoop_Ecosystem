# Day 10 Lab Guide — Create a Multi-Broker KRaft Kafka Cluster

In this hands-on lab, you will deploy a multi-broker, high-availability Apache Kafka cluster locally in **KRaft mode** (Kafka Raft Metadata mode) using Docker Compose, consisting of:
* **3 Kafka Brokers** (`kafka1-day10`, `kafka2-day10`, `kafka3-day10`) which run combined processes (both Broker and Controller roles).
* **Private Network** (`day10-network`) for inter-broker communication and metadata replication.
* **Persistent Volumes** for durability of the commit log directories.

You will verify broker bootstrapping, create partition-balanced topics, run ingestion workloads, simulate a broker crash, verify partition leader re-election, and observe cluster recovery.

---

## 🎯 Lab Objectives

1. Spin up a 3-broker KRaft cluster with a single Docker Compose command.
2. Verify KRaft metadata quorum election and metadata log state.
3. Create a topic with 3 partitions and a replication factor of 3.
4. Verify partition leadership distribution across brokers.
5. Simulate broker failure (stop a container) and verify that partition leadership is immediately reassigned to online replicas.
6. Restart the failed broker and verify it recovers, catches up on the write log, and rejoins the In-Sync Replicas (ISR) quorum.
7. Clean up the cluster resources.

---

## 💻 Prerequisites & Environment Setup

* **Docker & Docker Compose**: Ensure Docker Desktop is installed and running.
* **System Resources**: Reserve at least **4GB RAM** and **2 CPU cores** in Docker settings.
* **Operating System**: Linux, macOS, or Windows (WSL2/PowerShell).

---

## 🏁 Step-by-Step Lab Execution

### Step 1: Navigate to the Day 10 Directory
Open a terminal and navigate to the Day 10 directory:
```bash
cd Day-10-Kafka-Architecture
```

### Step 2: Spin up the 3-Broker Cluster
Run Docker Compose in detached mode to download the images and spawn the containers:
```bash
docker compose -f docker/docker-compose.yml up -d
```

**Expected Command Output:**
```text
[+] Running 5/5
 ✔ Network day10-network        Created
 ✔ Volume "kafka1-data"         Created
 ✔ Volume "kafka2-data"         Created
 ✔ Volume "kafka3-data"         Created
 ✔ Container kafka1-day10       Started
 ✔ Container kafka2-day10       Started
 ✔ Container kafka3-day10       Started
```

*Note: Allow **10–15 seconds** for the JVMs to initialize and the KRaft controllers to perform initial leader election and directory formatting using the cluster ID.*

---

### Step 3: Run the Verification Scripts

We have provided a set of diagnostic scripts under the `scripts/` directory to automate cluster validation.

#### 1. Verify Broker & Quorum Health
Verify that all 3 containers are healthy and running and that the KRaft consensus quorum is active:
```bash
bash scripts/verify-brokers.sh
```
Look for `LeaderId` in the output to see which broker was elected as the KRaft Controller Leader.

#### 2. Run the End-to-End Demo Script
Execute a complete ingestion cycle—creating a topic, producing keyed JSON messages, reading them back from the beginning, and printing offset states:
```bash
bash scripts/produce-consume-demo.sh
```

---

### Step 4: Manual Broker Failure Simulation

Now, you will manually simulate a production failure to observe Kafka's high-availability and fault-tolerance mechanics.

#### 1. Create a Replicated Topic
Create a new topic named `lab-ha-topic` with a replication factor of 3 and 3 partitions:
```bash
docker exec -it kafka1-day10 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic lab-ha-topic \
  --partitions 3 \
  --replication-factor 3
```

#### 2. Check Topic Layout
Describe the topic to see which brokers are the **Leaders**, where the **Replicas** reside, and the current **In-Sync Replicas (ISR)** list:
```bash
bash scripts/verify-topics.sh lab-ha-topic
```

**Expected Output (Identical or similar to):**
```text
Topic: lab-ha-topic	TopicId: ...	PartitionCount: 3	ReplicationFactor: 3	Configs: 
	Topic: lab-ha-topic	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
	Topic: lab-ha-topic	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
	Topic: lab-ha-topic	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
```
*Observe that the partitions are balanced across the 3 brokers.*

#### 3. Simulate Broker Crash
Stop the broker running on Node 3 (`kafka3-day10`):
```bash
docker compose -f docker/docker-compose.yml stop kafka3
```

#### 4. Verify Failure Recovery
Instantly query the replication health script:
```bash
bash scripts/verify-replication.sh
```
Observe that:
1. `kafka3-day10` is listed as not running.
2. Under-replicated partitions are reported because partition replicas on Node 3 are offline.
3. **Partition 2**, which had Broker 3 as its leader, has automatically elected a new leader (either Broker 1 or Broker 2).
4. The In-Sync Replicas (ISR) list for all partitions has dropped from `1,2,3` to `1,2`.

#### 5. Recover the Broker
Start the stopped broker back up:
```bash
docker compose -f docker/docker-compose.yml start kafka3
```

#### 6. Confirm Catch-up & Rejoin
Wait 5 seconds, then run the replication health check again:
```bash
bash scripts/verify-replication.sh
```
Observe that the under-replicated partition alarms are gone, Node 3 is back in the ISR list for all partitions, and the cluster is fully synchronized.

---

### Step 5: Clean Up
Tear down the containers and associated volumes:
```bash
docker compose -f docker/docker-compose.yml down -v
```
The `-v` flag ensures the persistent volumes are deleted, cleaning up your disk space.
