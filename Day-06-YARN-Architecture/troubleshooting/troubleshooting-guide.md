# Day 6 Troubleshooting Playbook — YARN Production Failures

This guide provides operational runbooks, log signatures, and step-by-step resolution commands for common failures encountered in YARN (Yet Another Resource Negotiator) clusters.

---

## 🚨 Scenario 1: ResourceManager is Down or Hanging

### 🔴 Symptoms
- YARN clients fail to submit jobs, yielding connection timeouts.
- YARN Web UI (`http://<rm-host>:8088`) returns `502 Bad Gateway` or `Connection Refused`.
- CLI commands like `yarn node -list` fail with:
  ```text
  Exception in thread "main" java.net.ConnectException: Connection refused
  ```

### 🔍 Root Causes
- Out of Memory (OOM) inside the ResourceManager JVM due to tracing a huge history of finished applications.
- Standby ResourceManager failover fails because the Active lock is orphaned in ZooKeeper.
- Disk capacity overflow on the RM log directory.

### 📝 Log Signature (`hadoop-yarn-resourcemanager-*.log`)
```text
2026-06-25 10:00:00,000 FATAL org.apache.hadoop.yarn.server.resourcemanager.ResourceManager: 
Shutting down ResourceManager due to java.lang.OutOfMemoryError: Java heap space
2026-06-25 10:01:05,000 WARN org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore: 
Connection lost to ZooKeeper quorum, retrying connection...
```

### 🛠️ Resolution Protocol
1. **Force Transition Standby to Active** (if running HA and failover got stuck):
   ```bash
   yarn rmadmin -transitionToActive --forcemanual rm1
   ```
2. **Clear Orphaned ZooKeeper Locks**:
   If ZKFC cannot gain the lock because of network partitions, clear it via ZK CLI:
   ```bash
   zkCli.sh -server zookeeper:2181
   # Inside zkCli:
   rmr /rmstore/ZKRMStateRoot
   ```
3. **Restart the ResourceManager Daemon**:
   ```bash
   yarn --daemon stop resourcemanager
   yarn --daemon start resourcemanager
   ```
4. **Tune RM Heap memory**: Increase `-Xmx` settings in `yarn-env.sh` (e.g., `export YARN_RESOURCEMANAGER_OPTS="-Xmx8g"`).

---

## 🚨 Scenario 2: NodeManager marked as "LOST"

### 🔴 Symptoms
- Active node count decreases in `verify-rm.sh` output.
- ResourceManager logs display warnings about node timeouts.
- Node is marked as `LOST` in the ResourceManager Web UI.

### 🔍 Root Causes
- NodeManager daemon crashed (OOM, OS level kill).
- Heartbeat packet loss due to excessive JVM garbage collection pauses on the NodeManager.
- Network connection broken on port `8031` (Resource Tracker RPC).

### 📝 Log Signature (`hadoop-yarn-resourcemanager-*.log` on RM)
```text
2026-06-25 10:15:00,000 INFO org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl: 
Node nodemanager-host:8041 reported State change from RUNNING to LOST due to EXPIRED
```

### 🛠️ Resolution Protocol
1. **Check NodeManager Process**: Log into the lost host and verify if the NodeManager is active:
   ```bash
   jps | grep NodeManager
   ```
2. **Check Port Connectivity** from NodeManager to ResourceManager:
   ```bash
   nc -zv resourcemanager-host 8031
   ```
3. **Analyze NodeManager GC Log**: Check if GC pauses exceed the expiry interval (default: 10 minutes):
   ```bash
   grep -i "gc" /var/log/hadoop-yarn/yarn-yarn-nodemanager-*.log | head -n 50
   ```
4. **Restart NodeManager Daemon**:
   ```bash
   yarn --daemon stop nodemanager
   yarn --daemon start nodemanager
   ```

---

## 🚨 Scenario 3: Container Killed due to Memory Limit Violation

### 🔴 Symptoms
- MapReduce or Spark tasks crash instantly.
- Job fails with container exit status `137` or `143` (killed by OS/YARN).
- Diagnostic logs show container was killed due to physical memory overflow.

### 🔍 Root Causes
- Spark/MapReduce JVM heap (`-Xmx`) is configured too close to, or exceeds, the YARN container allocation limit (`mapreduce.map.memory.mb` or `spark.executor.memory`).
- Non-heap overhead (off-heap memory, Python worker memory, native compression libs) exceeds the memory reserve.

### 📝 Log Signature (`hadoop-yarn-nodemanager-*.log` on NM)
```text
2026-06-25 10:30:00,000 WARN org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl: 
Container [pid=12345, containerID=container_1719545000000_0001_01_000002] is running beyond physical memory limits. 
Current usage: 1.05 GB of 1 GB physical memory used. Killing container.
```

### 🛠️ Resolution Protocol
1. **Verify Heap Ratio Rule**: Ensure the Java task Heap memory option is capped at **80% or less** of the total YARN container allocation.
   * If `mapreduce.map.memory.mb` is `1024`, then `mapreduce.map.java.opts` should be around `-Xmx819m`.
2. **Increase Container Allocation Limits**:
   In `configs/yarn-site.xml`, adjust maximum allocation bounds:
   ```xml
   <property>
       <name>yarn.scheduler.maximum-allocation-mb</name>
       <value>8192</value>
   </property>
   ```
3. **Increase Off-Heap Memory Allowances**:
   For Spark, increase the memory overhead factor:
   ```properties
   spark.executor.memoryOverhead=1024m
   ```
4. **Adjust Virtual Memory check**:
   If YARN kills containers due to virtual memory (vmem) checks (often triggered inside CentOS/Ubuntu running glibc memory allocations), disable it in `yarn-site.xml`:
   ```xml
   <property>
       <name>yarn.nodemanager.vmem-check-enabled</name>
       <value>false</value>
   </property>
   ```

---

## 🚨 Scenario 4: Scheduler Queue Starvation (Applications Pending)

### 🔴 Symptoms
- Submitted applications get stuck in `ACCEPTED` or `PENDING` states indefinitely.
- No containers are allocated to the new ApplicationMaster.
- ResourceManager UI lists "Available Resources" as greater than 0, yet jobs do not run.

### 🔍 Root Causes
- The leaf queue has reached its `maximum-capacity` limit.
- The user submitting the job has hit their user limit factor (`yarn.scheduler.capacity.root.<queue-name>.user-limit-factor`).
- The cluster is fully saturated running long-running containers (e.g., Spark thrift server, streaming jobs) that never release slots.

### 🛠️ Resolution Protocol
1. **Query Queue Allocations**:
   ```bash
   yarn queue -status production
   ```
2. **Identify Resource Consumers**:
   List active applications running in the stuck queue:
   ```bash
   yarn application -list -appStates RUNNING
   ```
3. **Preemption Configuration**: Enable preemption in `yarn-site.xml` so the scheduler can reclaim containers from queues exceeding their base capacity:
   ```xml
   <property>
       <name>yarn.resourcemanager.scheduler.monitor.enable</name>
       <value>true</value>
   </property>
   <property>
       <name>yarn.resourcemanager.scheduler.monitor.policies</name>
       <value>org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy</value>
   </property>
   ```
4. **Kill Rogue/Stuck Applications**:
   ```bash
   yarn application -kill <application_id>
   ```
