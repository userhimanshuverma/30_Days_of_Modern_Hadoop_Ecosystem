# Hands-On Lab: Building and Running WordCount from Scratch

This step-by-step guide explains how to compile, configure, deploy, run, and inspect the production-grade MapReduce WordCount job inside your local YARN cluster.

---

## 🛠️ Step 1: Compilation and Packaging with Maven

To build the MapReduce job, we compile our Java code against the Apache Hadoop Client library dependencies.

1. **Verify your local files**:
   Ensure you have `WordCount.java` and `pom.xml` under `/workspace/source` (which matches [WordCount.java](file:///d:/30_Days_of_Modern_Hadoop_Ecosystem/Day-14-MapReduce-Internals/source/WordCount.java) and [pom.xml](file:///d:/30_Days_of_Modern_Hadoop_Ecosystem/Day-14-MapReduce-Internals/source/pom.xml)).

2. **Trigger Compilation**:
   You can compile either on your host system if Maven is configured, or easily inside our containers which have Maven pre-installed. Run the compilation helper script:
   ```bash
   ./scripts/verify-wordcount.sh
   ```
   *Expected Output:*
   ```text
   === Compiling WordCount Java Project ===
   Running 'mvn clean package' inside namenode container...
   [INFO] Scanning for projects...
   [INFO] Building WordCount MapReduce Job 1.0-SNAPSHOT
   [INFO] --------------------------------[ jar ]---------------------------------
   [INFO] --- maven-compiler-plugin:3.11.0:compile (default-compile) @ wordcount-mapreduce ---
   [INFO] Compiling 1 source file to /workspace/source/target/classes
   [INFO] --- maven-jar-plugin:3.3.0:jar (default-jar) @ wordcount-mapreduce ---
   [INFO] Building jar: /workspace/source/target/wordcount-mapreduce-1.0-SNAPSHOT.jar
   [INFO] ------------------------------------------------------------------------
   [INFO] BUILD SUCCESS
   [SUCCESS] WordCount MapReduce JAR built successfully at: source/target/wordcount-mapreduce-1.0-SNAPSHOT.jar
   ```

---

## 🚀 Step 2: Spin Up the Hadoop Docker Cluster

If you haven't started your cluster, navigate to the `docker` directory and launch Docker Compose:

```bash
cd docker
docker compose up -d
```

Verify that all five daemons are running successfully:
```bash
docker ps
```
*Expected containers running:*
- `namenode-day14`
- `datanode-day14`
- `resourcemanager-day14`
- `nodemanager-day14`
- `historyserver-day14`

You can verify structural health by running:
```bash
./scripts/verify-hadoop.sh
```

---

## 📁 Step 3: Populate HDFS Input Data

Before executing the job, we must upload the source text dataset into our Hadoop Distributed File System (HDFS).

1. Create the `/input` directory in HDFS:
   ```bash
   docker exec namenode-day14 hdfs dfs -mkdir -p /input
   ```
2. Upload the mock sample text dataset `wordcount-input.txt`:
   ```bash
   docker exec namenode-day14 hdfs dfs -put /workspace/examples/wordcount-input.txt /input/
   ```
3. Verify the file exists in HDFS:
   ```bash
   docker exec namenode-day14 hdfs dfs -ls /input
   ```
   *Expected Output:*
   ```text
   Found 1 items
   -rw-r--r--   3 root supergroup        810 2026-07-05 19:50 /input/wordcount-input.txt
   ```

---

## 🏃 Step 4: Execute the MapReduce Job

Submit the packaged Java class to the YARN ResourceManager using the `yarn jar` launcher command:

```bash
docker exec namenode-day14 yarn jar /workspace/source/target/wordcount-mapreduce-1.0-SNAPSHOT.jar com.hadoop.mapreduce.WordCount /input /output
```

*Expected Terminal Log Flow:*
```text
2026-07-05 19:53:15,102 INFO client.RMProxy: Connecting to ResourceManager at resourcemanager/172.22.0.4:8032
2026-07-05 19:53:15,622 INFO input.FileInputFormat: Total input files to process : 1
2026-07-05 19:53:15,810 INFO mapreduce.JobSubmitter: number of splits:1
2026-07-05 19:53:16,115 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_169123456789_0001
2026-07-05 19:53:16,502 INFO impl.YarnClientImpl: Submitted application application_169123456789_0001
2026-07-05 19:53:16,610 INFO mapreduce.Job: The url to track the job: http://resourcemanager:8088/proxy/application_169123456789_0001/
2026-07-05 19:53:16,612 INFO mapreduce.Job: Running job: job_169123456789_0001
2026-07-05 19:53:23,980 INFO mapreduce.Job: Job job_169123456789_0001 running in uber mode : false
2026-07-05 19:53:23,982 INFO mapreduce.Job:  map 0% reduce 0%
2026-07-05 19:53:28,210 INFO mapreduce.Job:  map 100% reduce 0%
2026-07-05 19:53:34,510 INFO mapreduce.Job:  map 100% reduce 100%
2026-07-05 19:53:35,622 INFO mapreduce.Job: Job job_169123456789_0001 completed successfully
```

---

## 🔍 Step 5: Inspect execution stages

### 1. View Logs on YARN NodeManager
Check container execution output to trace the Mapper and Reducer setups and cleanups.
```bash
docker logs nodemanager-day14 | grep com.hadoop.mapreduce
```
*Expected log lines proving custom components triggered:*
```text
INFO com.hadoop.mapreduce.WordCount$TokenizerMapper: Initializing Mapper task: attempt_169123456789_0001_m_000000_0
INFO com.hadoop.mapreduce.WordCount$TokenizerMapper: Mapper task cleanup: attempt_169123456789_0001_m_000000_0
INFO com.hadoop.mapreduce.WordCount$IntSumReducer: Initializing Reducer task: attempt_169123456789_0001_r_000000_0
INFO com.hadoop.mapreduce.WordCount$IntSumReducer: Initializing Reducer task: attempt_169123456789_0001_r_000001_0
```

### 2. Inspect Intermediate Output
Because we set the number of reducer tasks to `2` and configured our custom `AlphabetPartitioner`:
- Words starting with `a-m` were partitioned to `part-r-00000.gz`
- Words starting with `n-z` were partitioned to `part-r-00001.gz`

Execute the validation script to automatically extract, decompress, and examine these outputs:
```bash
./scripts/verify-output.sh
```

---

## 🧹 Step 6: Cleanup

To clear HDFS space and spin down local container topologies:

1. Remove outputs from HDFS:
   ```bash
   docker exec namenode-day14 hdfs dfs -rm -r -f /input /output
   ```
2. Stop the docker containers:
   ```bash
   docker compose down -v
   ```
