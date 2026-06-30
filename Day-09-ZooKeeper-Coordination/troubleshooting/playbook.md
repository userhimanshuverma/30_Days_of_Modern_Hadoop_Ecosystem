# Production Troubleshooting Playbook — Apache ZooKeeper

This playbook contains operational instructions for diagnosing, mitigating, and resolving common errors, outages, and performance degradation in Apache ZooKeeper ensembles.

---

## 🛠️ Diagnostics & Debugging Command Kit

Always run these triage commands first when inspecting ZooKeeper nodes.

### 1. ZooKeeper 4-Letter Word Commands (4LW)
ZooKeeper exposes metrics and statuses through 4-letter string commands sent via `nc` (Netcat) or `telnet`. Ensure `4lw.commands.whitelist=*` or specific commands are enabled in `zoo.cfg`.

- **Check Node Health:**
  ```bash
  echo ruok | nc localhost 2181
  # Expected response: imok
  ```
- **Inspect Server Metrics and Mode:**
  ```bash
  echo stat | nc localhost 2181
  ```
- **Query Active Watches:**
  ```bash
  echo wchs | nc localhost 2181
  ```
- **Dump Client Sessions and Ephemeral Nodes:**
  ```bash
  echo cons | nc localhost 2181
  # Detailed metrics
  echo mntr | nc localhost 2181
  ```

### 2. Java Virtual Machine Diagnostics
Since ZooKeeper runs on the JVM, thread dumps and heap status are critical for diagnosing deadlocks, long GC pauses, and memory leaks.

- **Trigger Thread Dump (Identify Blocked/Deadlocked Threads):**
  ```bash
  jstack -l <zookeeper_pid> > /tmp/zk_threads.txt
  ```
- **Analyze Heap Usage (Check for memory leaks from watches):**
  ```bash
  jmap -histo:live <zookeeper_pid> | head -n 30
  ```

---

## 📋 Common Production Issues & Resolutions

### 1. Issue: No Leader Elected (Cluster in Standalone or Dead State)
- **Symptoms:**
  Clients receive `ConnectionLossException`. Querying `echo stat | nc localhost 2181` returns `This ZooKeeper instance is not running` or errors.
- **Root Cause:**
  - Connectivity failures on port 3888 (leader election port) preventing nodes from voting.
  - Quorum is lost; less than a majority of nodes configured in `zoo.cfg` are online (e.g., in a 3-node cluster, 2 nodes must be healthy. If 2 are down, no leader can be elected).
  - Mismatch or duplicate `myid` files.
- **Resolution:**
  1. Check container/process statuses of all nodes.
  2. Inspect the log file `zookeeper.log` on each node for `QuorumPeer` connection errors.
  3. Validate that peer names resolve to the correct IPs:
     ```bash
     nslookup zookeeper1
     ```
  4. Ensure port 3888 is open:
     ```bash
     nc -zvw3 zookeeper2 3888
     ```
  5. Restart the offline nodes to re-establish quorum.

---

### 2. Issue: Quorum Lost (More than Floor(N/2) Nodes Offline)
- **Symptoms:**
  The surviving nodes refuse to serve client requests (both read and write) and keep throwing connection exceptions.
- **Root Cause:**
  Hardware failures, network partitions, or disk failures taking down the majority of nodes.
- **Resolution:**
  1. If physical servers are unrecoverable, you must reconfigure the ensemble.
  2. **Force Standalone Boot (Emergency):** If only one node survived and you must recover data immediately, edit its `zoo.cfg` to comment out other servers, change it to standalone, and restart.
  3. Rebuild the failed instances from backup snapshots or restore network connectivity immediately to allow Zab synchronization.

---

### 3. Issue: Client Session Expired
- **Symptoms:**
  Clients log `Session expired` events, disconnect, and then ephemeral nodes created by that client disappear, causing services or locks to drop.
- **Root Cause:**
  - Client did not send a heartbeat within the negotiated `sessionTimeout` limit.
  - **JVM Garbage Collection (GC) pauses:** Long stop-the-world GC pauses on the ZooKeeper server or the client JVM freeze execution, causing heartbeats to skip.
  - High network latency or temporary packet loss.
- **Resolution:**
  1. Inspect client and server JVM logs for garbage collection pauses. Look for:
     ```text
     [GC (Allocation Failure) ... 2.5 secs]
     ```
  2. Optimize JVM GC settings. Switch to G1GC:
     ```text
     -XX:+UseG1GC -XX:MaxGCPauseMillis=50
     ```
  3. Increase client session timeout limits if operating on unstable networks (e.g., increase to 20-30 seconds).

---

### 4. Issue: Watches Not Triggering
- **Symptoms:**
  Metadata changes occur, but dependent client nodes do not receive updates.
- **Root Cause:**
  - ZooKeeper watches are **one-time triggers**. Once a watch is triggered, it is deleted. The client must re-register the watch upon receiving the event.
  - Network disconnect/reconnect event occurred; during the transition state, the watch might have been lost or triggered, and the client failed to handle the reconnection state listener correctly.
- **Resolution:**
  1. Audit client-side code to ensure watches are re-registered immediately inside the event handler.
  2. Use helper client libraries like Apache Curator (Java) or Kazoo (Python) which manage watch re-registration automatically.
  3. Verify watch counts on the server:
     ```bash
     echo wchs | nc localhost 2181
     ```

---

### 5. Issue: Split Brain in the Cluster
- **Symptoms:**
  Two different segments of the network claim to have elected distinct active leaders, leading to divergent states (split state).
- **Root Cause:**
  Improper ensemble sizing (even number of nodes) or network split without quorum checks.
- **Resolution:**
  - **Prevention (ZooKeeper Design):** ZooKeeper naturally prevents split brain because it relies on strict **Quorum Consensus**. A leader cannot be elected unless it secures votes from a majority of configured nodes:
    $$\text{Quorum} \ge \lfloor N/2 \rfloor + 1$$
    In a 3-node cluster, a majority is 2. If a partition divides the cluster into a 1-node segment and a 2-node segment, the 1-node segment cannot elect a leader, while the 2-node segment can.
  - **Remediation:** If split brain occurs due to faulty custom DNS configuration or manual host redirection, isolate the rogue leader container (`docker stop <rogue_node>`), fix hostname resolutions, and restart.

---

### 6. Issue: Slow Synchronization (Sync Limit Exceeded)
- **Symptoms:**
  Followers keep dropping out of the cluster, failing to sync with the leader.
- **Root Cause:**
  - Network throughput between the leader and followers is too low.
  - The transaction log write times on the leader or follower are extremely high, causing them to miss sync window heartbeats.
- **Resolution:**
  1. Increase the `syncLimit` parameter in `zoo.cfg` (e.g., from 5 to 10).
  2. Move the transaction log directory (`dataLogDir`) to a dedicated disk (e.g., SSD).
  3. Run network latency checks (`ping`, `traceroute`) between nodes.

---

### 7. Issue: Snapshot and Log File Corruption
- **Symptoms:**
  ZooKeeper fails to start up, printing `IOException: CRC check failed` or `EOFException` in logs.
- **Root Cause:**
  Sudden power outage, OS crash, disk failures, or disk running out of space during writing.
- **Resolution:**
  1. Identify the corrupt file in the log output (e.g., `log.100000001` or `snapshot.100000000`).
  2. Move the corrupt file out of the `dataDir` and `dataLogDir` to a backup location.
  3. ZooKeeper will automatically sync the missing transactions from other nodes in the ensemble upon startup.
  4. If all copies are corrupted, restore the directories from a backup snapshot.

---

### 8. Issue: High Latency and Disk Full
- **Symptoms:**
  Client operation latency increases dramatically. Write transactions timeout.
- **Root Cause:**
  - Transaction logs and snapshots are storing too many past historical instances, filling up the disk space.
  - Disk write queues are bottlenecked.
- **Resolution:**
  1. Check disk space usage:
     ```bash
     df -h
     ```
  2. Enable automatic log and snapshot purging in `zoo.cfg`:
     ```properties
     autopurge.snapRetainCount=5
     autopurge.purgeInterval=1
     ```
  3. If disk is already full, manually clean historical log files using the cleanup utility:
     ```bash
     java -cp zookeeper.jar org.apache.zookeeper.server.PurgeTxnLog <dataDir> <dataLogDir> -n 5
     ```
