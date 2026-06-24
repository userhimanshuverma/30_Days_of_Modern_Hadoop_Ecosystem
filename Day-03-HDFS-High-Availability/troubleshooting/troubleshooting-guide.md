# Day 3 Troubleshooting Playbook — HDFS HA Failures

This guide provides operational runbooks, log signatures, and step-by-step resolution commands for common failures encountered in HDFS High Availability environments.

---

## 🚨 Scenario 1: Split-Brain Condition (Dual Active NameNodes)

### 🔴 Symptoms
- HDFS clients receive conflicting block list info.
- Both `namenode1` and `namenode2` show `active` when querying:
  ```bash
  hdfs haadmin -getServiceState nn1
  hdfs haadmin -getServiceState nn2
  ```
- Major write failures or data corruption warnings in client logs.

### 🔍 Root Cause
- Network partition isolates `namenode1` from `namenode2` and ZooKeeper, preventing ZKFC from detecting that the other node is still alive.
- Failure of the fencing mechanism (e.g., SSH login fails due to changed credentials, or PDU switch port is unresponsive).

### 📝 Log Signature (`hadoop-hdfs-namenode-*.log`)
```text
2026-06-24 12:00:00,000 FATAL org.apache.hadoop.hdfs.server.namenode.ha.ZKFailoverController: 
Both NameNodesnn1 and nn2 are active! Fencing failed!
2026-06-24 12:00:01,000 ERROR org.apache.hadoop.hdfs.server.namenode.FSNamesystem: 
FSNamesystem write lock is held by another daemon!
```

### 🛠️ Resolution Protocol
1. **Force Isolation manually**: Immediately shut down or kill one of the NameNodes.
   ```bash
   # SSH into the rogue Active NameNode and kill the process
   kill -9 $(pgrep -f "org.apache.hadoop.hdfs.server.namenode.NameNode")
   ```
2. **Review Fencing Configuration**: Inspect `hdfs-site.xml` under `dfs.ha.fencing.methods`. Ensure that sshfence keys are valid and SSH ports are open between NameNodes.
3. **Verify ZooKeeper Node State**:
   ```bash
   # Run zkCli to verify the lock path
   zkCli.sh -server zookeeper:2181 ls /hadoop-ha/mycluster
   ```
4. **Transition to Standby**: If a node refuses to step down, use the manual override:
   ```bash
   hdfs haadmin -transitionToStandby --forcemanual nn1
   ```

---

## 🚨 Scenario 2: Loss of JournalNode Quorum (QJM Outage)

### 🔴 Symptoms
- Active NameNode crashes or transitions to Standby.
- Writes to HDFS fail instantly with "QJM Quorum Failed" errors.
- NameNode logs state they cannot sync edits.

### 🔍 Root Cause
- 2 out of 3 JournalNodes are offline or network isolated, making it impossible to form a majority write quorum ($\text{floor}(3/2) + 1 = 2$).

### 📝 Log Signature (`hadoop-hdfs-namenode-*.log`)
```text
2026-06-24 12:10:00,000 FATAL org.apache.hadoop.hdfs.qjournal.client.QuorumException: 
Got generic exception from 2 of 3 JournalNodes:
journalnode2:8485 - Connection refused
journalnode3:8485 - Connection refused
```

### 🛠️ Resolution Protocol
1. **Inspect JournalNode Status**: Check if JournalNode daemons are running on all hosts:
   ```bash
   # Run check inside Docker / VM hosts
   docker ps | grep journalnode
   ```
2. **Start the Daemons**:
   ```bash
   # On the hosts where JournalNodes are down:
   hdfs --daemon start journalnode
   ```
3. **Audit Network Accessibility**: Verify that port `8485` is reachable from both NameNodes using `nc` or `telnet`:
   ```bash
   nc -zv journalnode1 8485
   ```
4. **Verify Quorum Health**: Once at least 2 JournalNodes are online, the NameNode should automatically recover and resume operation. If not, restart the Active NameNode.

---

## 🚨 Scenario 3: ZKFC Ephemeral Lock Loss (Frequent Failovers)

### 🔴 Symptoms
- Cluster keeps switching Active/Standby status back and forth ("flapping").
- Performance degrades due to continuous state transition overhead.

### 🔍 Root Cause
- ZooKeeper session timeout occurs due to JVM garbage collection pauses on the NameNode host running ZKFC.
- Network congestion between ZKFC and ZooKeeper.

### 📝 Log Signature (`hadoop-hdfs-zkfc-*.log`)
```text
2026-06-24 12:20:00,000 WARN org.apache.zookeeper.ClientCnxn: 
Client session timed out, session id: 0x10000000a
2026-06-24 12:20:01,000 INFO org.apache.hadoop.ha.ActiveStandbyElector: 
Session expired. Resigning active state.
```

### 🛠️ Resolution Protocol
1. **Optimize JVM Garbage Collection**: Add GC flags to NameNode/ZKFC environments (`HADOOP_OPTS`) to reduce pauses:
   ```bash
   export HADOOP_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 $HADOOP_OPTS"
   ```
2. **Adjust Session Timeout**: Increase the ZooKeeper session timeout in `hdfs-site.xml` so brief network hiccups don't trigger failovers:
   ```xml
   <property>
       <name>ha.zookeeper.session-timeout.ms</name>
       <value>10000</value> <!-- Increase from 5000ms default to 10000ms -->
   </property>
   ```
3. **Monitor CPU & Disk Latency**: Ensure the NameNode host is not experiencing CPU starvation.

---

## 🚨 Scenario 4: Standby NameNode Fails to Bootstrap

### 🔴 Symptoms
- Standby NameNode container fails to start, looping with errors.
- Logs show that the Standby NameNode cannot synchronize state with the Active NameNode.

### 🔍 Root Cause
- The Standby NameNode `/hadoop/dfs/name` directory is empty but the bootstrap command has not run, or it was formatted with a different cluster/namespace ID.
- Active NameNode RPC port 9000 is not reachable from Standby NameNode.

### 📝 Log Signature
```text
2026-06-24 12:30:00,000 FATAL org.apache.hadoop.hdfs.server.namenode.NameNode: 
Directory /hadoop/dfs/name is not formatted.
2026-06-24 12:30:01,000 ERROR org.apache.hadoop.hdfs.server.namenode.NameNode: 
Inconsistent NamespaceID! Standby has: 12345, Active has: 67890
```

### 🛠️ Resolution Protocol
1. **Check RPC Connectivity**: Ensure you can resolve and connect to the active NameNode:
   ```bash
   docker exec -it namenode2-day03 nc -zv namenode1 9000
   ```
2. **Execute Manual Bootstrap**: Run the bootstrap command on the Standby NameNode to copy the Active NameNode's current fsimage:
   ```bash
   docker exec -it namenode2-day03 hdfs namenode -bootstrapStandby -force
   ```
3. **Restart the Standby NameNode daemon**:
   ```bash
   docker restart namenode2-day03
   ```
