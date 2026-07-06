# Day 15 Lab Guide: Executing Queries on Apache Tez

In this hands-on lab, you will deploy a local single-node cluster hosting Hadoop (HDFS/YARN), Apache Tez, and Apache Hive. You will compile a native Tez Java application, query Hive using both MapReduce and Tez, and measure the performance gains.

---

## 🛠️ Prerequisites
- Docker and Docker Compose installed.
- Maven installed locally (if compiling code outside the container, though the container includes Maven).
- Standard terminal access.

---

## 🚀 Step 1: Clone and Start the Cluster
Navigate to the docker folder and launch the containers:
```bash
cd docker
docker-compose up -d
```
Verify all containers (`namenode`, `datanode`, `resourcemanager`, `nodemanager`, `hiveserver2`) are running and healthy:
```bash
docker-compose ps
```

---

## 🔬 Step 2: Access the Gateway Shell
Open an interactive bash shell in the `hiveserver2` gateway container:
```bash
docker exec -it hiveserver2-day15 /bin/bash
```
The workspace code directory will be mounted inside `/workspace`.

---

## ⚡ Step 3: Run the Auto-Verification Scripts
Run the Tez classpath validation script:
```bash
cd /workspace/scripts
./verify-tez.sh
```
Run the Hive-on-Tez engine configuration test:
```bash
./verify-hive-tez.sh
```

---

## 📊 Step 4: Run the Hive-on-Tez vs. MapReduce Benchmark
To run the benchmark query comparing Hive execution times under MapReduce versus Tez:
```bash
./run-tez-demo.sh
```
This script will:
1. Populate a test table (`benchmark_data`) with 20,000 generated records.
2. Run an analytical grouping aggregation query on **MapReduce** and record the elapsed time.
3. Run the exact same query on **Apache Tez** and record the elapsed time.
4. Output the results side-by-side.

---

## ☕ Step 5: Execute the Custom Java Tez DAG Job
We will compile and run the native `TezWordCount.java` DAG job on YARN.

1. Compile the Maven project:
   ```bash
   cd /workspace/source
   mvn clean package
   ```
2. Put a test file into HDFS:
   ```bash
   hadoop fs -mkdir -p /input
   echo "Apache Tez is a high-performance DAG execution engine." > /tmp/test.txt
   echo "Tez is much faster than Hadoop MapReduce due to in-memory piping." >> /tmp/test.txt
   hadoop fs -put /tmp/test.txt /input/
   ```
3. Run the compiled Tez jar on YARN:
   ```bash
   hadoop jar target/tez-demo-app-1.0-SNAPSHOT.jar com.hadoop.tez.TezWordCount /input/test.txt /output-tez
   ```
4. Verify the outputs in HDFS:
   ```bash
   hadoop fs -cat /output-tez/part*
   ```

---

## 🧹 Step 6: Cluster Tear Down
To stop the environment and release all volume resources:
```bash
exit
cd ../docker
docker-compose down -v
```
This cleans up network routing, temporary HDFS configurations, and Derby metastores.
```
