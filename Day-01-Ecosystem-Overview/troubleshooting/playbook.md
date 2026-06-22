# Day 1: Production Troubleshooting Playbook

This playbook provides solutions to common failures and startup issues that may occur when deploying the Day 1 Hadoop ecosystem container stack.

---

## 📋 Troubleshooting Table

| Issue | Symptoms | Root Cause | Key Logs | Resolution |
|---|---|---|---|---|
| **Kafka Not Starting** | Container crashes immediately with error code 1 or keeps restarting. | Unable to connect to ZooKeeper; or host advertised listener address is misconfigured. | `Connection to node -1 ... could not be established. Broker may not be available.` or `Configured zookeeper.connect may be wrong.` | 1. Ensure `zookeeper-day01` is running (`docker ps`).<br>2. Check if Zookeeper port `2181` is open.<br>3. Verify Kafka's `KAFKA_ZOOKEEPER_CONNECT` environment variable resolves to `zookeeper:2181`. |
| **HDFS Safemode Block** | HDFS writes fail with error: `SafeModeException: Cannot create directory ... NameNode is in safe mode.` | NameNode is protecting block data from metadata loss. It starts in Safe Mode by default and waits for DataNodes to report their blocks. If replication factor is too high, it hangs in safe mode. | `NameNode in safe mode ... The ratio of reported blocks ... is less than the threshold.` | 1. Check DataNode status using `docker compose logs datanode`. <br>2. Manually force NameNode to leave Safe Mode:<br>`docker exec -it namenode-day01 hdfs dfsadmin -safemode leave` |
| **Hive Metastore Unavailable** | HiveServer2 or Spark jobs fail to query catalog. Beeline throws `Could not open connection to jdbc:hive2://...:10000`. | PostgreSQL DB starting slowly, or Metastore schema is not initialized yet. | `org.apache.hadoop.hive.metastore.api.MetaException: Could not connect to meta store using any of the URIs...` | 1. Check `hive-metastore-db-day01` logs.<br>2. Check Metastore container logs.<br>3. Ensure DB initialized properly. If database metadata is corrupt, wipe the volume and restart:<br>`docker compose down -v && docker compose up -d` |
| **Spark Executor Failures** | Spark jobs are submitted, but they hang indefinitely or workers are terminated. | Docker host has run out of memory, killing JVM processes (OOM Killer). | `ExecutorLostFailure (executor 0 exited caused by one of the running tasks)` or `Killed` in system daemon logs. | 1. Open Docker Desktop settings.<br>2. Increase Memory allocation to at least **8GB**.<br>3. Lower Spark worker memory allocation inside `docker-compose.yml` to `1G` (default is `2G`). |
| **Disk Space Exhausted** | HDFS writes fail, NameNode UI logs errors. Docker daemon fails to start containers. | Host SSD/HDD is full, causing HDFS or Postgres databases to reject writes. | `java.io.IOException: No space left on device` | 1. Clear unused Docker resources:<br>`docker system prune -a --volumes`<br>2. Wipe existing HDFS test directories:<br>`docker exec namenode-day01 hdfs dfs -rm -r -f /tmp/*` |
| **Container Networking Partition** | Services cannot resolve names like `namenode` or `kafka` inside containers. | Containers did not join the same user-defined network bridge. | `java.net.UnknownHostException: namenode` | 1. Ensure all services have `networks: - day01-network` defined.<br>2. Run `docker network inspect day01-network` to check if all containers are listed inside the network subnet. |

---

## 🛠️ Step-by-Step Debugging Playbook

### 1. Forcing HDFS out of Safemode

When HDFS starts up, NameNode waits for DataNodes to report their block locations. If NameNode does not receive reports from enough DataNodes within a timeout, it blocks all write operations.

If you are running in a single DataNode sandbox environment, sometimes the block reports fall below the threshold. Run the command below to force it to leave safe mode:

```bash
docker exec -it namenode-day01 hdfs dfsadmin -safemode leave
```

---

### 2. Initializing or Resetting Hive Metastore Database

If your Hive Metastore fails due to database connection or missing table exceptions, you can manually initialize or reset the schema:

1. Exec into the Hive Metastore container:
   ```bash
   docker exec -it hive-metastore-day01 bash
   ```
2. Run the schema tool to initialize schema for PostgreSQL:
   ```bash
   /opt/hive/bin/schematool -dbType postgres -initSchema
   ```
3. Exit and restart the service containers:
   ```bash
   docker compose restart hive-metastore hive-server
   ```

---

### 3. Check Open Ports on Host Machine

If you get a bind error during `docker compose up -d` (e.g., `Port 5432 is already in use`), it means you have local database or messaging engines running on your host machine.

- **Check port 5432 (Postgres):** A local Postgres server might be running on Windows/Linux. Stop your local PostgreSQL service before launching the docker environment.
- **Check port 9092 (Kafka):** Stop any local Kafka brokers.
- **Check port 2181 (Zookeeper):** Stop any local Zookeeper instances.
- **Check port 8080 (Spark Web UI):** A local Tomcat, Node, or Jenkins app might be running on 8080. You can change the port mapping in `docker-compose.yml` to `"8085:8080"` for `spark-master`.
