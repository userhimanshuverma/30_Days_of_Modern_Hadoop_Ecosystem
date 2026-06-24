# Day 3 Lab Guide — Simulate HDFS NameNode Failover

In this hands-on lab, you will deploy a multi-node, high-availability HDFS cluster locally using Docker Compose, consisting of:
* 1 ZooKeeper Coordination Instance
* 3 JournalNodes (edits quorum)
* 2 NameNodes (Active & Standby, each running a ZKFC side-daemon)
* 2 DataNodes (Workers)

You will then verify daemon orchestration, run write tests using the logical cluster URI (`hdfs://mycluster`), simulate a NameNode crash, and observe automatic failover.

---

## 🎯 Lab Objectives

1. Spin up a multi-component HDFS High Availability cluster with a single command.
2. Verify JournalNode quorum synchronization and ZooKeeper locks.
3. Perform logical nameservice I/O operations.
4. Simulate an Active NameNode failure (kill container) and verify that the Standby NameNode is promoted to Active.
5. Recover the failed NameNode and verify it rejoins safely as a Standby node.

---

## 💻 Prerequisites & Environment Setup

* **Docker & Docker Compose**: Ensure Docker Desktop is installed and running.
* **System Resources**: Reserve at least **8GB RAM** and **4 CPU cores** in Docker settings.
* **Operating System**: Linux, macOS, or Windows (WSL2/PowerShell).

---

## 🏁 Step-by-Step Lab Execution

### Step 1: Navigate to the Day 3 Directory
Open a terminal and navigate to the Day 3 directory:
```bash
cd Day-03-HDFS-High-Availability
```

### Step 2: Boot up the Cluster
Spin up all services in the background:
```bash
docker compose -f docker/docker-compose.yml up -d
```

**Expected Command Output:**
```text
[+] Running 9/9
 ✔ Network day03-network             Created                                                   0.0s
 ✔ Container zookeeper-day03         Started                                                   1.0s
 ✔ Container journalnode1-day03      Started                                                   1.5s
 ✔ Container journalnode2-day03      Started                                                   1.6s
 ✔ Container journalnode3-day03      Started                                                   1.4s
 ✔ Container namenode1-day03         Started                                                   2.2s
 ✔ Container namenode2-day03         Started                                                   3.1s
 ✔ Container datanode1-day03         Started                                                   3.8s
 ✔ Container datanode2-day03         Started                                                   3.7s
```

*Note: The containers automatically wait, format ZooKeeper, format JournalNodes, and bootstrap the Standby NameNode. Give it **20-30 seconds** for all services to coordinate and transition.*

---

### Step 3: Run the Verification Scripts

We have provided a set of diagnostic scripts under the `scripts/` directory to verify component health.

#### 1. Verify ZooKeeper Locks
Verify that the ZKFC locks are active in ZooKeeper:
```bash
bash scripts/verify-zookeeper.sh
```

#### 2. Verify JournalNode Quorums
Confirm that all 3 JournalNodes are running and synchronized:
```bash
bash scripts/verify-journalnodes.sh
```

#### 3. Verify Active/Standby Roles
Check which NameNode was elected Active:
```bash
bash scripts/verify-active-standby.sh
```

#### 4. Run Master HA Health Checks
Orchestrate all verification phases and execute logical read/write operations:
```bash
bash scripts/verify-ha.sh
```

---

### Step 4: Interacting with the Logical Namespace

HDFS clients interact with the logical nameservice URI `hdfs://mycluster` instead of individual hostnames.

#### 1. Write a File to HDFS
Run a client write command using the logical namespace path:
```bash
docker exec -it namenode1-day03 hdfs dfs -put /etc/hadoop/core-site.xml hdfs://mycluster/system/ha-test.xml
```

#### 2. Query Namespace state
List the root folders inside HDFS:
```bash
docker exec -it namenode2-day03 hdfs dfs -ls hdfs://mycluster/system/
```

---

### Step 5: Simulate NameNode Failover

Now, we will simulate a real failover. Run the automated failover check:
```bash
bash scripts/verify-failover.sh
```

#### Manual Failover Run-through:
1. Identify the Active NameNode (e.g. `namenode1-day03`).
2. Stop the active container:
   ```bash
   docker stop namenode1-day03
   ```
3. Read the HDFS file using the logical namespace URI from the remaining standby NameNode container:
   ```bash
   docker exec -it namenode2-day03 hdfs dfs -cat hdfs://mycluster/system/ha-test.xml
   ```
   *Notice how the read succeeds instantly! ZKFC has promoted `namenode2` to Active, and client requests route transparently.*
4. Start the failed NameNode back up:
   ```bash
   docker start namenode1-day03
   ```
5. Check states:
   ```bash
   bash scripts/verify-active-standby.sh
   ```
   *Notice how `namenode1-day03` safely joins as Standby, avoiding any split-brain conflicts.*

---

### Step 6: Cleanup Environment
Once the lab is complete, shut down and purge all containers, networks, and persistent volume data:
```bash
docker compose -f docker/docker-compose.yml down -v
```
