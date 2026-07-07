# Day 16: Apache Spark Core Standalone Cluster Hands-On Lab
# Location: Day-16-Spark-Core-Architecture/labs/lab-guide.md

## 📌 Lab Scenario
In this lab, you will spin up a multi-node Standalone Spark cluster with 1 Master, 2 Workers, and a Spark History Server integrated with HDFS. You will compile and run a custom Java Spark WordCount application, inspect job lineages, analyze tasks/stages in the Spark Web UI, and verify executor memory structures.

---

## 🛠️ Step-by-Step Execution Guide

### Step 1: Start the Docker Infrastructure
Navigate to the docker directory and launch the containers:
```bash
cd /workspace/docker
docker-compose up -d
```
Verify that all containers are healthy:
```bash
docker-compose ps
```
*Expected Output:*
- `namenode-day16` (Healthy)
- `datanode-day16` (Healthy)
- `spark-master-day16` (Healthy)
- `spark-worker-1-day16` (Healthy)
- `spark-worker-2-day16` (Healthy)
- `spark-history-day16` (Healthy)
- `spark-client-day16` (Up)

---

### Step 2: Access Spark Client and Run Cluster Verification
Connect to the client shell:
```bash
docker exec -it spark-client-day16 /bin/bash
```

Run the built-in verification scripts to validate cluster components:
```bash
cd /workspace/scripts

# 1. Verify ports and connection endpoints
./verify-spark-cluster.sh

# 2. Verify driver environment configurations
./verify-driver.sh

# 3. Verify executor allocation limits and active worker count
./verify-executors.sh
```

---

### Step 3: Run the Java Spark Job
Submit the custom Spark WordCount application to the cluster:
```bash
./run-spark-demo.sh
```
This script will:
1. Compile the Maven Java project in `source/`.
2. Generate mock input text and load it into HDFS.
3. Submit the compiled jar (`spark-demo-app-1.0-SNAPSHOT.jar`) using `spark-submit`.
4. Output the word counts partitioned across 3 output files (due to the `HashPartitioner(3)`).

---

### Step 4: Verify Outputs in HDFS
List the files in the output directory:
```bash
hadoop fs -ls /output-spark
```
*Expected Output:*
```text
/output-spark/_SUCCESS
/output-spark/part-00000
/output-spark/part-00001
/output-spark/part-00002
```
Read the partitioned data:
```bash
hadoop fs -cat /output-spark/part-00000 | head -n 10
```

---

### Step 5: Explore the Spark Web UIs

Open your browser on the host system to inspect the cluster metrics:
1. **Spark Master UI (`http://localhost:8080`)**: Check registered workers, active applications, CPU core counts, and executor memory splits.
2. **Spark Worker UIs (`http://localhost:8081` and `http://localhost:8082`)**: Inspect running executors, directories, logs (`stdout`/`stderr`), and task threads.
3. **Spark History Server (`http://localhost:18080`)**: Review completed applications, jobs list, stage DAG diagrams, event timelines, and task execution tables.

---

### Step 6: Shutdown & Cleanup
Once finished, exit the client shell and tear down the containers:
```bash
exit
docker-compose down -v
```
The `-v` flag removes the HDFS data volumes to keep your host environment clean.
