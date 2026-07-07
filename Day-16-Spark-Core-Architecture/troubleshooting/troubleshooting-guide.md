# Day 16: Apache Spark Core Production Troubleshooting Playbook
# Location: Day-16-Spark-Core-Architecture/troubleshooting/troubleshooting-guide.md

This playbook contains symptoms, root causes, diagnostic strategies, and resolution steps for debugging Apache Spark Core workloads in staging and production clusters.

---

## 🚨 1. Executor Lost / Disassociated (Symptom: ExecutorLostFailure)

### Symptom
- Spark jobs fail with errors like:
  ```text
  ERROR TaskSetManager: Task 4 in stage 2.0 failed 4 times; aborting job
  org.apache.spark.SparkException: Job aborted due to stage failure: Task 4 in stage 2.0 failed 4 times...
  ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Container killed by YARN for exceeding memory limits.
  ```
- Or on Standalone cluster:
  ```text
  WARN TaskSchedulerImpl: Lost executor 1 on worker-host: Executor heartbeat timed out after 120000 ms
  ```

### Root Cause
1. **YARN Memory Overhead Violations**: The executor's physical memory footprint (JVM heap + off-heap/native memory + thread stack) exceeded the YARN container allocation. The NodeManager sent a SIGKILL signal.
2. **Heavy Garbage Collection (GC) Pauses**: Long GC cycles freeze the executor JVM, causing it to fail to send heartbeat signals to the Driver, leading the Driver to assume the executor is dead.
3. **Physical Host Issues**: Out of memory (OOM killer) at the OS level or networking splits.

### Resolution
1. **Increase Memory Overhead**: Set `spark.executor.memoryOverhead` to at least **10% to 15%** of `spark.executor.memory` (minimum 384MB).
   ```properties
   spark.executor.memoryOverhead   512m
   ```
2. **Tune Garbage Collection**: Switch to G1GC and set target pause times:
   ```properties
   spark.executor.extraJavaOptions  -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:InitiatingHeapOccupancyPercent=35
   ```
3. **Increase Heartbeat Timeouts**: In heavily loaded networks, increase heartbeat interval settings:
   ```properties
   spark.network.timeout            800s
   spark.executor.heartbeatInterval 60s
   ```

---

## 🧠 2. Driver Out Of Memory (OOM) Errors (Symptom: java.lang.OutOfMemoryError: Java heap space)

### Symptom
- The Driver JVM crashes or halts with:
  ```text
  FATAL SparkContext: Uncaught exception in thread SparkListenerBus
  java.lang.OutOfMemoryError: Java heap space
  ```
- Application client shuts down during aggregation or result retrieval.

### Root Cause
1. **Unbounded `collect()` Calls**: Running `.collect()` on a massive distributed RDD attempts to transfer all partitions across the network and buffer them into the single Driver's memory.
2. **Large Broadcast Variables**: Broadcasting very large lookup datasets (`sc.broadcast(large_map)`) exceeds the Driver JVM heap size.
3. **Heavy Metadata Trackers**: Having millions of small partitions creates a massive metadata map that the Driver's DAG Scheduler must track in heap space.

### Resolution
1. **Avoid `collect()`**: Use `take(n)` to sample a few rows, or save results directly to storage (e.g. `saveAsTextFile()`, `write.save()`).
2. **Increase Driver Memory**: Increase Driver size when using broadcast maps or compiling massive execution graphs:
   ```properties
   spark.driver.memory   4g
   ```
3. **Coalesce/Repartition**: If data has too many partitions (e.g., millions of files), use `.coalesce()` or adjust source configurations to group data blocks.

---

## ⚡ 3. Shuffle Fetch Failures (Symptom: FetchFailedException)

### Symptom
- Stage execution fails with:
  ```text
  org.apache.spark.shuffle.FetchFailedException: Failed to connect to host:port
  at org.apache.spark.storage.ShuffleBlockFetcherIterator.throwFetchFailedException
  ```
- The DAG Scheduler repeatedly retries the parent Stage, causing a loop.

### Root Cause
1. **Executor Crash**: The upstream Executor holding the shuffle map files crashed (often due to OOM), so the downstream task cannot fetch the records.
2. **Network Timeouts / Garbage Collection**: The source executor was unresponsive because of heavy CPU loading or GC freeze during the fetch request.
3. **Local Disk Space Exhaustion**: Standalone Workers or YARN NodeManagers ran out of local disk space to write shuffle temp files.

### Resolution
1. **Diagnose Upstream Failures**: Examine the logs of the lost executor at the timestamp of the first fetch failure to fix the underlying OOM or worker crash.
2. **Enable External Shuffle Service**: This allows shuffle data to be served by NodeManager daemons even if the Spark executor JVM has finished or crashed:
   ```properties
   spark.shuffle.service.enabled   true
   spark.dynamicAllocation.enabled true
   ```
3. **Adjust Shuffle Network Limits**: Increase buffer sizes and network retries:
   ```properties
   spark.shuffle.io.maxRetries     10
   spark.shuffle.io.retryWait      30s
   ```

---

## 📊 4. Skewed Partitions & Straggler Tasks

### Symptom
- Most tasks in a stage finish in 2 seconds, but 1 or 2 tasks hang for minutes.
- Cluster resource utilization is high but progress is blocked.

### Root Cause
- **Data Skew**: The partition key (e.g., null values, empty strings, or very frequent ID codes) is unevenly distributed, concentrating millions of records into a single task slot while other task slots process only a few rows.

### Resolution
1. **Salting the Key**: Append a random integer suffix to skewed keys before grouping, aggregate them, then strip the suffix to do the final aggregation.
2. **Filter Skewed Keys**: If skewed keys represent null values, filter them out before doing joins.
3. **Adjust Parallelism**: Increase the shuffle partition counts to distribute records across more tasks:
   ```properties
   spark.default.parallelism          200
   spark.sql.shuffle.partitions       200
   ```

---

## 🧬 5. Serialization Errors (Symptom: NotSerializableException)

### Symptom
- Job submission fails immediately on the client with:
  ```text
  java.io.NotSerializableException: org.apache.spark.SparkContext
  Serialization stack:
	- object not serializable (class: org.apache.spark.SparkContext, value: org.apache.spark.SparkContext@...)
  ```

### Root Cause
- Spark transmits functions (lambdas, map tasks) to Executors across the network. If the closure references a class that does not implement `java.io.Serializable` (such as the `SparkContext`, database connections, or loggers), serialization fails.

### Resolution
1. **Mark Fields as `transient`**: If a class property cannot be serialized, declare it as transient so Spark skips it during serialization.
2. **Instantiate Connections Locally**: Instantiate un-serializable objects (like database clients or loggers) inside the task block (e.g., inside `mapPartitions` instead of outside).
3. **Use Kryo Serializer**: It is faster and handles complex objects better than default Java serialization:
   ```properties
   spark.serializer               org.apache.spark.serializer.KryoSerializer
   spark.kryo.registrator         my.custom.KryoRegistrator
   ```

---

## 🛠️ 6. Debugging Command Sheet

### Fetch Spark Logs on YARN
```bash
yarn logs -applicationId <application_id>
```

### Inspect Spark Standalone Master Logs
Check files inside the Spark installation log directory on the Master host:
```bash
cat /opt/spark/logs/spark--org.apache.spark.deploy.master.Master-*.out
```

### Check Active Threads and Stack Trace
Use `jstack` on the Worker nodes to check for deadlocks in long-running Spark tasks:
```bash
jstack <executor_pid> > /tmp/executor_thread_dump.txt
```
