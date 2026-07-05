# Production Troubleshooting Playbook: MapReduce Internals

This playbook outlines common issues encountered when executing MapReduce jobs at scale on YARN, with real-world symptoms, root causes, diagnostic commands, and resolutions.

---

## 💥 Scenario 1: Mapper / Reducer JVM OutOfMemoryError (OOM)

### Symptoms
YARN terminates the container. The job logs show:
```text
Container [pid=12345,containerID=container_169000_0001_01_000002] is running beyond physical memory limits. Current usage: 2.1 GB of 2 GB physical memory used. Killing container.
```
Or in the task stdout/stderr:
```text
java.lang.OutOfMemoryError: Java heap space
```

### Root Cause
1. **Physical Limit Exceeded**: The physical JVM heap usage exceeded the resource capacity allocated by YARN (`mapreduce.map.memory.mb` or `mapreduce.reduce.memory.mb`).
2. **Heap-to-Container Mismatch**: The Java heap configuration (`-Xmx`) was too close to or exceeded the container size, leaving no room for off-heap allocations (DFS client buffers, native code, sorting metadata).

### Resolution
1. **Increase Container Allocations**:
   Increase memory in `mapred-site.xml` or via CLI parameters:
   ```bash
   yarn jar my-job.jar -Dmapreduce.map.memory.mb=4096 -Dmapreduce.map.java.opts="-Xmx3276m" ...
   ```
   Ensure `-Xmx` is always configured to **75-80%** of the container limit (`mapreduce.map.memory.mb`).
2. **Optimize GC parameters**:
   Add JVM garbage collection optimizations:
   ```xml
   <property>
       <name>mapreduce.map.java.opts</name>
       <value>-Xmx3276m -XX:+UseG1GC</value>
   </property>
   ```

---

## ⏳ Scenario 2: Reducer Data Skew (99% Complete Hang)

### Symptoms
The MapReduce job progresses quickly to `map 100% reduce 99%` and then halts. One or two Reducer tasks run for hours while others complete in seconds.

### Root Cause
An uneven distribution of intermediate key-value pairs. A disproportionately large amount of data is associated with a single partition/key (e.g., null values, empty strings, or a massive category in telemetry data), routing it all to a single Reducer.

### Resolution
1. **Implement a Combiner**: Aggregates identical intermediate keys locally on the Mapper node before transmitting data over the network, dramatically reducing payload size.
   ```java
   job.setCombinerClass(IntSumReducer.class);
   ```
2. **Custom Partitioner**:
   Implement a custom partitioner to detect skewed keys and distribute them using salting (adding a random suffix like `key_0`, `key_1` to distribute them across multiple reducers).
3. **Change Partitioning Key**: If possible, partition by a higher-cardinality composite key rather than a low-cardinality state or boolean value.

---

## ⚡ Scenario 3: Container Killed Beyond Virtual Memory (VMEM) Limits

### Symptoms
Tasks fail instantly on local dev clusters or cloud nodes with:
```text
Container [pid=89231,containerID=container_...] is running beyond virtual memory limits. Current usage: 4.3 GB of 2.0 GB virtual memory used. Killing container.
```

### Root Cause
Modern Linux distributions and JVMs allocate large swaths of virtual memory addresses (especially when using glibc's memory allocation layout). YARN NodeManager checks the ratio of virtual to physical memory (`yarn.nodemanager.vmem-pmem-ratio`, default is `2.1`). If the JVM reserves more virtual addresses than the multiplier allows, YARN forcefully terminates it, even if physical usage is minimal.

### Resolution
1. **Disable VMEM verification** (highly recommended for developer Docker clusters or cloud setups with Glibc overhead):
   Add the following property in `yarn-site.xml`:
   ```xml
   <property>
       <name>yarn.nodemanager.vmem-check-enabled</name>
       <value>false</value>
   </property>
   ```
2. **Increase the Ratio**: If security policies require verification, increase the ratio:
   ```xml
   <property>
       <name>yarn.nodemanager.vmem-pmem-ratio</name>
       <value>5.0</value>
   </property>
   ```

---

## 🌐 Scenario 4: Shuffle Fetch Failures (Connection Timeouts)

### Symptoms
Reducers fail during the Shuffle stage, throwing errors like:
```text
Shuffle Error: org.apache.hadoop.mapreduce.task.reduce.Shuffle$ShuffleError: error in shuffle in fetcher#1
Caused by: java.io.IOException: Exceeded maxConnectionsPerHost: 15
```
Or:
```text
2026-07-05 19:57:00,105 WARN [fetcher#1] org.apache.hadoop.mapreduce.task.reduce.Fetcher: Failed to connect to host nodemanager1: Connection refused
```

### Root Cause
1. **NodeManager Under Load**: The target NodeManager hosting intermediate map outputs is too busy or has hit thread limits, causing its local Netty/Shuffle HTTP handler to reject requests.
2. **Auxiliary Service Missing**: The YARN shuffle auxiliary service is not configured or failed to start in `yarn-site.xml`.

### Resolution
1. **Verify Shuffle auxiliary settings in `yarn-site.xml`**:
   ```xml
   <property>
       <name>yarn.nodemanager.aux-services</name>
       <value>mapreduce_shuffle</value>
   </property>
   <property>
       <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
       <value>org.apache.hadoop.mapred.ShuffleHandler</value>
   </property>
   ```
2. **Tune Shuffle Handler Threads**:
   Increase Netty server worker threads in `mapred-site.xml` to handle heavy parallel fetches:
   ```xml
   <property>
       <name>mapreduce.shuffle.max.connections</name>
       <value>100</value>
   </property>
   <property>
       <name>mapreduce.reduce.shuffle.parallelcopies</name>
       <value>20</value>
   </property>
   ```

---

## 🛠️ Essential Debugging CLI Commands

Use these production CLI diagnostics tools to locate issues quickly:

| Action | Command |
| :--- | :--- |
| **List running applications** | `yarn application -list` |
| **Retrieve job execution logs** | `yarn logs -applicationId application_169123456789_0001` |
| **Inspect job status** | `mapred job -status job_169123456789_0001` |
| **List Map/Reduce task attempts** | `mapred job -list-attempt-ids job_169123456789_0001 MAP RUNNING` |
| **Kill a runaway job** | `mapred job -kill job_169123456789_0001` |
| **View NodeManager list** | `yarn node -list` |
