# Day 2 Troubleshooting Guide — HDFS Architecture & Operations

This document provides detailed diagnosis steps, log analysis signatures, and mitigation playbooks for the most common administrative and operational failures encountered in Apache HDFS clusters.

---

## 🚦 Phase 1: Essential Diagnostic Tools

Before attempting to resolve failures, you must gather telemetry. These three command-line utilities are your primary tools:

### 1. The Global Health Audit (`hdfs fsck`)
Evaluates the integrity of the filesystem by traversing the namespace and checking block locations.
```bash
# Check the entire filesystem health
hdfs fsck /

# Check a specific file and show its block addresses and locations
hdfs fsck /path/to/file -files -blocks -locations

# Identify files containing corrupt or missing blocks
hdfs fsck / -list-corruptfileblocks
```

### 2. The Administrator Portal (`hdfs dfsadmin`)
Performs administrative control operations and displays health states.
```bash
# Display a summary cluster storage report
hdfs dfsadmin -report

# Retrieve the current status of Safe Mode
hdfs dfsadmin -safemode get

# Forcefully exit Safe Mode (Warning: review root causes first!)
hdfs dfsadmin -safemode leave

# Refresh node membership configuration without restarting NameNode
hdfs dfsadmin -refreshNodes
```

### 3. Log Inspection
Locate Hadoop daemon logs, typically written to `/var/log/hadoop` or `/opt/hadoop/logs` inside standard containers.
* **NameNode Logs:** Target file is `hadoop-hdfs-namenode-<hostname>.log`.
* **DataNode Logs:** Target file is `hadoop-hdfs-datanode-<hostname>.log`.

---

## 🛠️ Phase 2: Operational Incident Playbooks

### Playbook 1: NameNode OutOfMemory (OOM)
* **Symptoms:** NameNode process crashes, prints `java.lang.OutOfMemoryError: Java heap space` in logs, and HDFS becomes completely unresponsive.
* **Root Cause:** NameNode JVM Heap memory is fully exhausted. Since NameNode keeps the entire HDFS directory tree and block-to-DataNode mappings in RAM, a massive volume of metadata (especially due to the "Small File Problem") will blow out the heap. HDFS requires approximately **1GB of JVM Heap per 1 million blocks**.
* **Remediation Steps:**
  1. Increase the heap allocation. Locate `hadoop-env.sh` (or set `HADOOP_NAMENODE_OPTS` / `HADOOP_HEAPSIZE` environment variables):
     ```bash
     export HADOOP_NAMENODE_OPTS="-Xms8g -Xmx8g ${HADOOP_NAMENODE_OPTS}"
     ```
  2. Restart the NameNode service:
     ```bash
     # In docker
     docker compose restart namenode
     ```
  3. Run audits to identify users generating small files and schedule Spark/Hive compaction processes to merge small partitions.

### Playbook 2: NameNode Stuck in Safe Mode
* **Symptoms:** Clients receive `org.apache.hadoop.hdfs.server.namenode.SafeModeException: Cannot write to HDFS. NameNode is in safe mode.`
* **Root Cause:** Safe Mode is a read-only state. On startup, the NameNode loads the `fsimage` metadata and waits for DataNodes to register and upload their Block Reports. If the percentage of reported blocks doesn't reach the threshold (`dfs.namenode.safemode.threshold-pct`, default is `99.9%`), NameNode remains locked in Safe Mode to prevent block corruption.
* **Remediation Steps:**
  1. Check the safe mode status and report details:
     ```bash
     hdfs dfsadmin -safemode get
     hdfs dfsadmin -report
     ```
  2. Inspect why DataNodes are down. If a major rack went offline, restart the DataNodes in that rack.
  3. If you have confirmed that the missing blocks are permanently lost and are willing to accept data loss on those files to return the rest of the cluster to service, run:
     ```bash
     hdfs dfsadmin -safemode leave
     ```
  4. Run `hdfs fsck / -delete` to delete files associated with the missing, unrecoverable blocks.

### Playbook 3: Missing or Corrupt Blocks
* **Symptoms:** `hdfs fsck` reports "CORRUPT BLOCKS" or "MISSING BLOCKS". Read operations on affected files fail with `BlockMissingException`.
* **Root Cause:** A block is "Missing" when zero replicas are online (all DataNodes holding copies of that block are offline or crashed). A block is "Corrupt" if its checksum validation fails during client reads or DataNode background scanners.
* **Remediation Steps:**
  1. List all corrupt files:
     ```bash
     hdfs fsck / -list-corruptfileblocks
     ```
  2. If the blocks are missing because DataNodes are temporarily disconnected, resolve DataNode container/network failures and wait for them to register.
  3. If the blocks are permanently lost (e.g. disk failure on nodes without replication), you must delete the corrupt files:
     ```bash
     hdfs dfs -rm /path/to/corrupt/file
     # Or let fsck clean up corrupt files
     hdfs fsck / -delete
     ```

### Playbook 4: Under-Replicated Blocks
* **Symptoms:** `hdfs fsck` reports "Under-replicated blocks". DFSAdmin shows block counts under-replicated.
* **Root Cause:** A DataNode hosting replicas has crashed, or the Replication Factor configuration was increased, meaning the active replication count is lower than the target configured replication factor.
* **Remediation Steps:**
  1. HDFS automatically repairs this! The NameNode schedules background copy tasks from the remaining replicas to healthy DataNodes.
  2. Check if copy limits are throttling replication speed:
     ```xml
     <!-- hdfs-site.xml configuration to tune replication speed -->
     <property>
       <name>dfs.namenode.replication.max-streams</name>
       <value>20</value>
     </property>
     ```
  3. Check if all DataNodes are active. If a node is down, starting it will immediately resolve the under-replication.

### Playbook 5: DataNode Disk Full
* **Symptoms:** DataNodes shut down or log `IOException: No space left on device`. DFSAdmin shows remaining space as 0%.
* **Root Cause:** Non-DFS data or HDFS files have consumed all disk storage allocations.
* **Remediation Steps:**
  1. Configure HDFS disk reservation to prevent OS lockups:
     ```xml
     <!-- Reserve 10GB for non-HDFS operations on the local disk -->
     <property>
       <name>dfs.datanode.du.reserved</name>
       <value>10737418240</value>
     </property>
     ```
  2. Run the Balancer to move blocks to under-utilized DataNodes:
     ```bash
     # Start rebalancer with a 10% threshold
     hdfs balancer -threshold 10
     ```
  3. Delete obsolete files or adjust TTL/expirations on tables.
