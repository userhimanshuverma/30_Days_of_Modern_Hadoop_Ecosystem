# Day 5 Troubleshooting Guide: HDFS Performance Optimization

This guide details common performance bottlenecks in production HDFS clusters, their symptoms, root causes, relevant log traces, and practical CLI resolutions.

---

## 1. Slow Reads (High Latency or Low Throughput)

### Symptoms
* Spark/Hive scan tasks take much longer than expected.
* Active clients show network bottlenecks or high CPU wait times.
* WebHDFS read operations time out or stall.

### Root Cause
* **Lack of Data Locality**: Task trackers are scheduled on different hosts than the targeted HDFS blocks, forcing large data transfers over the network switch.
* **Disabled Short-Circuit Local Reads**: Local clients are routing read requests through the local DataNode's TCP loopback socket, adding serialization overhead instead of reading directly from raw storage.

### Diagnostic Command
Check the percentage of local vs. remote reads in NameNode statistics:
```bash
curl -s http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeVolumeImperatives
```
Check if Short-Circuit Local Reads are succeeding by reviewing client-side log files:
```text
2026-06-26 18:00:00,000 INFO org.apache.hadoop.hdfs.BlockReaderFactory: Client local read shortcircuit is enabled and succeeding.
```

### Resolution
1. Ensure the following configurations are added to `hdfs-site.xml` on the client and DataNodes:
   ```xml
   <property>
       <name>dfs.client.read.shortcircuit</name>
       <value>true</value>
   </property>
   <property>
       <name>dfs.domain.socket.path</name>
       <value>/var/lib/hadoop-hdfs/dn_socket</value>
   </property>
   ```
2. Verify that the directory hosting the domain socket has strict permissions (`755`) and is owned by `hdfs:hadoop`. If permissions are too open (e.g., `777`), short-circuit reads will fail for security reasons:
   ```bash
   chmod 755 /var/lib/hadoop-hdfs
   chown hdfs:hadoop /var/lib/hadoop-hdfs
   ```

---

## 2. Slow Writes and Pipeline Stalls

### Symptoms
* MapReduce/Spark job output stages freeze.
* DataNode logs print "Slow write pipeline" or socket write timeouts.
* Write operations fail with `IOException: All replicas are down`.

### Root Cause
* **Slow disk I/O on a single replica node**: Because HDFS write pipeline requires synchronous replication (DN1 -> DN2 -> DN3), the speed of the pipeline is determined by the slowest disk in the chain.
* **Exhausted DataNode Transceiver Threads**: If `dfs.datanode.max.transfer.threads` is set too low, the DataNode cannot handle incoming connections.

### Log Traces
Look in the DataNode daemon logs (`/var/log/hadoop/hadoop-hdfs-datanode-*.log`):
```text
2026-06-26 18:05:12,125 WARN org.apache.hadoop.hdfs.server.datanode.DataNode: Slow write pipeline: block BP-12948123-127.0.0.1-1718237192:blk_1073741825_1001 took 2548ms to write packet.
2026-06-26 18:05:15,312 ERROR org.apache.hadoop.hdfs.server.datanode.DataNode: xceivers queue size 8192 exceeded.
```

### Resolution
1. Increase the maximum transfer thread count in `hdfs-site.xml`:
   ```xml
   <property>
       <name>dfs.datanode.max.transfer.threads</name>
       <value>8192</value>
   </property>
   ```
2. Identify the slow physical disks using OS-level commands on the flagged DataNodes:
   ```bash
   iostat -xz 1 10
   ```
   Look for disks with high utilization percentage (`%util`) and high service wait times (`await`). Replace failing disk hardware or remove slow mount paths from `dfs.datanode.data.dir`.

---

## 3. NameNode JVM Pause and GC Bottlenecks

### Symptoms
* Clients receive frequent `SocketTimeoutException` or `Retrying connect to server` errors when contacting the NameNode.
* Heartbeats are missed, causing healthy DataNodes to be falsely flagged as dead.
* CPU utilization on the NameNode host spikes to 100% across multiple cores.

### Root Cause
* **Stop-the-World (STW) Garbage Collection pauses**: With large namespace counts (tens of millions of files/blocks), Java's Parallel GC is unable to clean objects without pausing the JVM for seconds or minutes.

### Log Traces
Check NameNode logs or GC logs:
```text
2026-06-26 18:10:45,918 INFO org.apache.hadoop.util.JvmPauseMonitor: Detected pause of 4235ms. JVM GC may be thrashing.
```

### Resolution
1. Tune NameNode JVM settings in `hadoop-env.sh` to transition to **G1GC** with target pause deadlines:
   ```bash
   export HADOOP_NAMENODE_OPTS="-Xms8g -Xmx8g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:InitiatingHeapOccupancyPercent=45 -XX:G1ReservePercent=15"
   ```
2. Clean up small files to reduce JVM memory fragmentation (see Section 4).

---

## 4. NameNode Namespace Bloat (Small Files Problem)

### Symptoms
* NameNode JVM memory usage is near 100% capacity despite low physical block space utilization.
* NameNode startup takes hours during FSImage loading and block report processing.

### Root Cause
* **Excessive metadata overhead**: Storing millions of tiny files (each < 1MB) consumes the same amount of NameNode memory namespace pointers (approx. 150 bytes for the inode and 150 bytes per block reference) as large files, wasting gigabytes of RAM.

### Diagnostic Command
Check the average file size and block count:
```bash
hdfs dfsadmin -report
hdfs fsck / -blocks
```

### Resolution
1. Consolidate existing small files into **SequenceFiles**, **Hadoop Archives (HAR)**, or convert ingestion formats to Parquet/ORC.
2. Build a HAR archive using:
   ```bash
   hadoop archive -archiveName myarchive.har -p /user/raw_data /user/archives
   ```
3. Set up file consolidation pipelines (e.g., Spark scripts running `.coalesce()` or `.repartition()`) prior to archiving in HDFS.

---

## 5. HDFS Balancer Running Extremely Slow

### Symptoms
* Balancer command is executed but is transferring less than 1 GB per hour.
* Dynamic cluster load does not equalize across DataNodes.

### Root Cause
* **Throttled Balancer Bandwidth limit**: The default bandwidth limit is set to 1MB/s (`dfs.datanode.balance.bandwidthPerSec = 1048576`), which is far too low for modern multi-terabyte drives.

### Resolution
1. Dynamically increase balancer bandwidth on the active cluster (no restart required):
   ```bash
   hdfs dfsadmin -setBalancerBandwidth 104857600  # 100 MB/s
   ```
2. Launch the balancer with optimized parameters to parallelize blocks transfers:
   ```bash
   hdfs balancer -threshold 5 -dispatcherThreads 10 -maxConcurrentMoves 50
   ```
   * `-threshold 5`: Balance nodes until standard deviation is within 5%.
   * `-maxConcurrentMoves 50`: Increase the threads moving blocks concurrently on each DataNode.
