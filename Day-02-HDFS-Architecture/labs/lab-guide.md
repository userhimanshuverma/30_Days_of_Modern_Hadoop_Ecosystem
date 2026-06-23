# Day 2 Lab Guide — Create HDFS Cluster Locally

In this hands-on lab, you will deploy a multi-service HDFS cluster locally using Docker Compose, consisting of one NameNode and three DataNodes. You will then run diagnostic health checks and learn fundamental HDFS operations.

---

## 🎯 Lab Objectives

1. Start a local containerized HDFS cluster with 1 NameNode and 3 DataNodes.
2. Execute diagnostic scripts to verify daemon configurations and connectivity.
3. Perform standard HDFS filesystem operations (create directories, write files, inspect replicas, adjust replication).
4. Run dynamic fault-tolerance tests by stopping a DataNode and monitoring block recovery.

---

## 💻 Prerequisites & Environment Setup

* **Docker & Docker Compose**: Ensure Docker Desktop is installed and running.
* **System Resources**: Reserve at least **8GB RAM** and **4 CPU cores** in Docker Desktop settings.
* **Operating System**: Linux, macOS, or Windows with WSL2/PowerShell.

---

## 🏁 Step-by-Step Lab Execution

### Step 1: Clone and Navigate to the Lab Folder
Open a terminal and navigate to the Day 2 folder:
```bash
cd Day-02-HDFS-Architecture
```

### Step 2: Spin Up the Cluster
Launch the NameNode and three DataNode containers in the background using Docker Compose:
```bash
docker compose -f docker/docker-compose.yml up -d
```

**Expected Command Output:**
```text
[+] Running 5/5
 ✔ Network day02-network          Created                                                      0.0s
 ✔ Container namenode-day02       Started                                                      1.1s
 ✔ Container datanode1-day02      Started                                                      1.5s
 ✔ Container datanode2-day02      Started                                                      1.4s
 ✔ Container datanode3-day02      Started                                                      1.5s
```

### Step 3: Wait for Initialization and Run Health Checks
Give the JVM services approximately 15-20 seconds to boot and register. Then run the NameNode verification script:
```bash
bash scripts/verify-namenode.sh
```

**Expected Output:**
```text
=== HDFS NameNode Diagnostics ===
[OK] Container 'namenode-day02' is running.

Checking port bindings...
[OK] NameNode is listening on RPC port 9000 (Internal IPC)
[OK] NameNode is listening on Web UI port 9870 (HTTP)

Querying NameNode JMX Endpoint...

=== NameNode Info ===
Hadoop Version:      3.2.1
Safe Mode Status:    Safe mode is OFF
Total HDFS Capacity: 62 GB
Used HDFS Capacity:  0 GB
Free HDFS Capacity:  48 GB
Live DataNodes:      3
Dead DataNodes:      0

[SUCCESS] NameNode health verification completed successfully. All 3 DataNodes are registered and active.
```

Next, verify that all three DataNodes are online:
```bash
bash scripts/verify-datanodes.sh
```

---

### Step 4: Interact with HDFS via CLI

Let's log in to the NameNode container or run commands remotely using `docker exec`.

#### 1. Create a Directory
Create a structured folder path inside HDFS:
```bash
docker exec -it namenode-day02 hdfs dfs -mkdir -p /user/curriculum/data
```

#### 2. Upload a File
Let's upload the configuration `core-site.xml` from the container's disk to HDFS:
```bash
docker exec -it namenode-day02 hdfs dfs -put /etc/hadoop/core-site.xml /user/curriculum/data/
```

#### 3. List the Directory Contents
Verify the file was written:
```bash
docker exec -it namenode-day02 hdfs dfs -ls /user/curriculum/data/
```

**Expected Output:**
```text
Found 1 items
-rw-r--r--   3 root supergroup        809 2026-06-23 13:00 /user/curriculum/data/core-site.xml
```
*Note: The number `3` in the second column indicates the current Replication Factor of the file.*

#### 4. Read the File Content
Read the uploaded file directly from HDFS:
```bash
docker exec -it namenode-day02 hdfs dfs -cat /user/curriculum/data/core-site.xml
```

---

### Step 5: Test Replication and Fault Tolerance

Run the automated replication verification script:
```bash
bash scripts/verify-replication.sh
```
This script creates a mock 10KB file, uploads it with replication factor 3, runs `fsck` to show which individual DataNodes hold the block replicas, and then downsizes the replication dynamically to 2 and 1 to observe HDFS cleanup.

#### Manual Fault Tolerance Test:
1. Upload a large file (e.g. 50MB) or a test file with replication 3.
2. Stop one DataNode container:
   ```bash
   docker stop datanode3-day02
   ```
3. Immediately run the health audit:
   ```bash
   bash scripts/verify-hdfs-health.sh
   ```
   You will notice HDFS reports `Live DataNodes: 2` and lists blocks as `Under-replicated`.
4. Wait 1-2 minutes. Start the DataNode container back up:
   ```bash
   docker start datanode3-day02
   ```
5. Check health again; HDFS will automatically clear Safe Mode warnings and heal the replication factors.

---

### Step 6: Cleanup Environment
Once you are finished, stop and destroy the containers and delete volume mounts:
```bash
docker compose -f docker/docker-compose.yml down -v
```
