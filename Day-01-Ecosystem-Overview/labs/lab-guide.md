# Day 1 Lab Guide: Deploying the Hadoop Ecosystem Sandbox

This lab guides you through deploying a complete containerized data platform environment containing Zookeeper, Kafka, HDFS NameNode, HDFS DataNode, PostgreSQL (Hive Metadata database), Hive Metastore, HiveServer2, Spark Master, and Spark Worker.

---

## 💻 Hardware Prerequisites

Because you are spinning up a multi-container distributed system sandbox, ensure your machine meets these specifications:

| Requirement | Minimum | Recommended |
|---|---|---|
| **RAM** | 8 GB | 16 GB+ |
| **CPU Cores** | 4 Cores | 8 Cores+ |
| **Disk Space** | 10 GB Free | 20 GB+ Free (SSD preferred) |
| **OS** | Windows (WSL2), macOS, Linux | Linux (Ubuntu/CentOS) or macOS |

> [!WARNING]
> If you are using Docker Desktop on Windows or macOS, ensure you increase the resource allocation limits inside Docker settings. Allocate at least **6-8 GB of RAM** and **4 CPU cores** to Docker, otherwise JVM services (like Hive and NameNode) will crash with out-of-memory errors.

---

## 🛠️ Step 1 — Startup All Containers

Navigate to the `Day-01-Ecosystem-Overview/docker` directory:

```bash
cd Day-01-Ecosystem-Overview/docker
```

Spin up the cluster in the background (detached mode):

```bash
docker compose up -d
```

---

## 🔍 Step 2 — Monitor Cluster Startup Status

Since services have dependencies (e.g., Hive Metastore waits for HDFS NameNode and Postgres DB to start), it may take 30–60 seconds for the entire cluster to initialize.

Check which containers are running:

```bash
docker compose ps
```

*Expected output:*
You should see all 9 containers with a status of `Up` (or `running`).

```
NAME                         IMAGE                                                COMMAND                  SERVICE             CREATED             STATUS              PORTS
datanode-day01               bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8     "/entrypoint.sh /run…"   datanode            10 seconds ago      Up 9 seconds        0.0.0.0:9864->9864/tcp
hive-metastore-day01         bde2020/hive:2.3.2-postgresql-metastore              "entrypoint.sh /opt/…"   hive-metastore      10 seconds ago      Up 9 seconds        0.0.0.0:9083->9083/tcp
hive-metastore-db-day01      bde2020/hive-metastore-postgresql:2.3.0              "docker-entrypoint.s…"   hive-metastore-db   10 seconds ago      Up 9 seconds        0.0.0.0:5432->5432/tcp
hive-server-day01            bde2020/hive:2.3.2-postgresql-metastore              "entrypoint.sh /opt/…"   hive-server         10 seconds ago      Up 9 seconds        0.0.0.0:10000->10000/tcp, 0.0.0.0:10002->10002/tcp
kafka-day01                  confluentinc/cp-kafka:7.2.1                          "/etc/confluent/dock…"   kafka               10 seconds ago      Up 9 seconds        0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp
namenode-day01               bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8      "/entrypoint.sh /run…"   namenode            10 seconds ago      Up 9 seconds        0.0.0.0:9000->9000/tcp, 0.0.0.0:9870->9870/tcp
spark-master-day01           bitnami/spark:3.2.1                                  "/opt/bitnami/script…"   spark-master        10 seconds ago      Up 9 seconds        0.0.0.0:7077->7077/tcp, 0.0.0.0:8080->8080/tcp
spark-worker-day01           bitnami/spark:3.2.1                                  "/opt/bitnami/script…"   spark-worker        10 seconds ago      Up 9 seconds        0.0.0.0:8081->8081/tcp
zookeeper-day01              zookeeper:3.8.0                                      "/docker-entrypoint.…"   zookeeper           10 seconds ago      Up 9 seconds        0.0.0.0:2181->2181/tcp
```

---

## 📋 Step 3 — Inspect Container Logs

To debug any service that failed to start, inspect its standard logs:

```bash
docker compose logs -f <service_name>
```

Example: Inspecting NameNode startup logs:

```bash
docker compose logs -f namenode
```

---

## 🧪 Step 4 — Run Automated Validation Scripts

We have provided automated bash scripts inside the `scripts/` directory to verify each service:

1. **Verify HDFS Storage Cluster:**
   ```bash
   bash ../scripts/verify-hadoop.sh
   ```
   *Action:* Creates a local test file, uploads it to HDFS NameNode, reads it back, checks for matches, and cleans up.

2. **Verify Spark Execution Cluster:**
   ```bash
   bash ../scripts/verify-spark.sh
   ```
   *Action:* Copies a python script to the Spark Master container, submits it to run on the worker executor, and prints result count.

3. **Verify Kafka Messaging Broker:**
   ```bash
   bash ../scripts/verify-kafka.sh
   ```
   *Action:* Creates a validation topic, publishes a test message, consumes it back within a timeout, and deletes the topic.

4. **Verify Hive SQL Query Engine:**
   ```bash
   bash ../scripts/verify-hive.sh
   ```
   *Action:* Establishes Beeline JDBC connection to HiveServer2, creates a test database and table, inserts data, queries it, drops table, and drops database.

---

## 🧹 Step 5 — Environment Cleanup

When finished with Day 1, tear down the environment to release memory and CPU resources:

```bash
docker compose down
```

To remove all persistent HDFS volumes and databases (for a clean slate next time):

```bash
docker compose down -v
```
