# Production Troubleshooting Playbooks: Apache Pinot

This document provides step-by-step diagnostic workflows, common log patterns, and remediation steps for issues encountered while running Apache Pinot clusters at scale.

---

## 🧭 Troubleshooting Cheat Sheet

```
                          [ Pinot Cluster Issue ]
                                     |
         +---------------------------+---------------------------+
         |                           |                           |
[ Ingestion Stopped ]       [ High Query Latency ]      [ Component Failures ]
         |                           |                           |
  - Check ZooKeeper           - Check Segment Pruning     - Controller Split-Brain
  - Check Kafka offsets       - Verify Star-tree index    - Server Out of Memory
  - Inspect Server logs       - Check Deep storage path   - Broker OOM / Heap Limit
```

---

## 1. Broker Unavailable

### Symptom
Applications receive `503 Service Unavailable`, connection timeouts, or `No broker found for table` when querying the Pinot Broker REST API.

### Root Causes
* **Heap Exhaustion**: Broker JVM crashed due to Out of Memory (OOM) error caused by heavy queries pulling large result sets.
* **Helix Mismatch**: Broker lost connection to ZooKeeper, causing Helix to mark the Broker instance as offline.
* **Network Partition**: Health-check ports blocked or load-balancer routes failing.

### Log Signatures
In `pinot-broker.log`:
```
java.lang.OutOfMemoryError: Java heap space
  at org.apache.pinot.core.common.datatable.DataTableImplV3.<init>(DataTableImplV3.java)
```
Or:
```
WARN [HelixManager] [HelixManager-Broker] Disconnected from Zookeeper. Lost session ID.
ERROR [BrokerInstance] [Broker] Cannot route query: No active servers available for table user_registrations.
```

### Resolution Playbook
1. **Check Process Status**: Check if the JVM is running:
   ```bash
   jcmd | grep Broker
   ```
2. **Examine Heap Usage**: If crashed, analyze GC logs. Increase broker heap size in `JAVA_OPTS`:
   ```bash
   -Xms4G -Xmx4G
   ```
3. **Verify Zookeeper Connection**: Ping Zookeeper from the Broker host to confirm routing.
4. **Inspect Helix State**: Verify the Broker registration using Pinot Controller Swagger/REST API:
   ```bash
   curl -X GET http://localhost:9000/instances
   ```

---

## 2. Segment Loading Failure

### Symptom
Segments remain in `ERROR` or `DOWN` state inside the Controller console. Queries targeting specific time windows return incomplete results (partial data scan).

### Root Causes
* **Disk Space Saturation**: Server local disk is full, preventing it from downloading and unpacking segments.
* **Corrupt Segments**: Segment metadata is unreadable, or segment file was corrupted during push to Deep Storage.
* **Deep Storage Unreachable**: S3, HDFS, or MinIO permissions changed, making it impossible to read/download historical files.

### Log Signatures
In `pinot-server.log`:
```
ERROR [SegmentDataManager] [user_registrations_OFFLINE_1720932000_1720935600_0] Failed to load segment.
java.io.IOException: No space left on device
  at java.io.FileOutputStream.writeBytes(Native Method)
```
Or:
```
ERROR [LLCRealtimeSegmentDataManager] [user_registrations_REALTIME__0__0__20260715T1300Z] Failed to download segment from S3: AccessDeniedException
```

### Resolution Playbook
1. **Check Server Disk Capacity**: Log in to the server and run `df -h`. If disk utilization is above 90%, expand volume size or clean up old segments.
2. **Validate Deep Storage connectivity**: Run a test file transfer from the Server instance using `aws s3 cp` or `hadoop fs -ls` depending on storage layer.
3. **Trigger Segment Reload**: Force the Controller to instruct the Server to download the segment again:
   ```bash
   curl -X POST "http://localhost:9000/tables/user_registrations/segments/user_registrations_OFFLINE_segment_name/reload"
   ```

---

## 3. Kafka Ingestion Stopped

### Symptom
Real-time ingestion lag grows linearly. The table is out-of-date compared to Kafka stream, but query execution is otherwise healthy.

### Root Causes
* **Kafka Offset Reset**: The offset requested by the Server was deleted (expired) from Kafka (retention breach).
* **Kafka Credentials Expired**: SASL/SCRAM credentials expired or truststore certificate failed TLS verification.
* **Helix LLC (Low Level Consumer) Lock**: Realtime segment consumption is stuck in `CONSUMING` state due to partition rebalancing.

### Log Signatures
In `pinot-server.log`:
```
WARN [LLCRealtimeSegmentDataManager] [user_registrations_REALTIME__1__0__20260715T1330Z] Segment consumption stopped.
org.apache.kafka.common.errors.OffsetOutOfRangeException: The requested offset is not within the range of offsets maintained by the server.
```
Or:
```
ERROR [KafkaConsumer] SSL handshake failed: Certificate has expired
```

### Resolution Playbook
1. **Check Ingestion Status**: Retrieve current ingestion offsets and compare with Kafka's end offsets:
   ```bash
   curl -X GET "http://localhost:9000/tables/user_registrations_REALTIME/consumingSegmentsInfo"
   ```
2. **Force Offset Reset**: If offsets are out of range, set consumer config `"stream.kafka.consumer.prop.auto.offset.reset"` to `"smallest"` or `"largest"`, then trigger reload.
3. **Re-align LLC Segments**: Restart the Server node that is lagging to force segment state machine recalculation.

---

## 4. High Query Latency

### Symptom
Queries that previously returned in 5ms now take 2000ms. Server CPU utilization spikes to 100%.

### Root Causes
* **Missing Index**: A filter column is missing an inverted index or range index, forcing Pinot to perform a full-scan (linear evaluation) over segment arrays.
* **Incorrect Segment Pruning**: Time-column queries do not specify start/end limits, scanning every historical segment.
* **Heap Trashing**: Frequent GC pauses slow query pipelines.

### Log Signatures
In `pinot-broker.log` or Slow Query Log:
```
INFO [QueryLogger] Query: SELECT COUNT(*) FROM user_registrations WHERE age = 30
Took: 1845ms, numDocsScanned: 50000000, numSegmentsQueried: 450, numSegmentsProcessed: 450
```
> [!NOTE]
> Observe that `numDocsScanned` matches total table records, and `numSegmentsQueried` is high, indicating NO indexes were used.

### Resolution Playbook
1. **Analyze Query Plan**: Run the query with `EXPLAIN PLAN FOR` prefixed:
   ```sql
   EXPLAIN PLAN FOR SELECT COUNT(*) FROM user_registrations WHERE age = 30
   ```
   Inspect the execution operator tree. Look for linear scans vs index lookups.
2. **Add Inverted Index**: Update the table config `user-registrations-table-realtime.json` under `invertedIndexColumns` to include the filtered dimension, then trigger table reload.
3. **Verify Time Columns**: Always include a time range filter (e.g., `signupTimestamp > 1781520000000`) in client queries to enable **Segment Pruning** (where the Broker skips segments outside the time boundary).

---

## 5. Missing Realtime Data

### Symptom
Data is visible in Kafka and can be consumed, but queries against the Pinot table do not return the newest events.

### Root Causes
* **Clock Drift**: Host system clock of the Pinot Server or Kafka broker is out-of-sync, causing events to be indexed under future/past timestamps.
* **Malformed JSON Payload**: Incoming messages cannot be decoded, causing Pinot to skip events or fail the segment.

### Log Signatures
In `pinot-server.log`:
```
ERROR [LLCRealtimeSegmentDataManager] Skip parsing row. Decoded JSON is empty or invalid.
org.apache.pinot.spi.data.readers.GenericRow$ParseException: Failed to parse timestamp field
```

### Resolution Playbook
1. **Inspect Dead Letter Records**: Check server logs for validation and decoding errors.
2. **Verify JSON Formats**: Ensure that the producers emit messages that match the schema data types exactly.
3. **Time Column Alignment**: If timestamps are formatted as string dates (e.g., `YYYY-MM-DD`), ensure your schema specifies the correct datetime parsing expression in `dateTimeFieldSpecs`.

---

## 6. Controller Election Failure (Split-Brain)

### Symptom
Multiple Controller nodes claim to be the Leader, or no Leader is elected. Cluster configuration updates (like schema registrations) fail or hang.

### Root Causes
* **ZooKeeper Session Loss**: Heavy JVM GC pause on Leader Controller causes it to miss ZooKeeper heartbeat. ZooKeeper deletes ephemeral node, and starts new election.
* **Network Partition**: Zookeeper nodes can't reach each other (quorum loss) or controllers can't reach Zookeeper.

### Log Signatures
In `pinot-controller.log`:
```
FATAL [ControllerLeaderLocator] Lost connection to Zookeeper. Session expired. Relinquishing leadership.
ERROR [HelixManager] Helix manager failed to connect. Retrying...
```

### Resolution Playbook
1. **Verify ZooKeeper Quorum Status**: Check Zookeeper status:
   ```bash
   echo stat | nc localhost 2181
   ```
   Ensure at least `(n/2) + 1` nodes are active and healthy.
2. **Locate Leader Path**: Query ZooKeeper for the leader controller:
   ```bash
   docker exec pinot-zookeeper bin/zkCli.sh get /pinot-cluster/CONTROLLER/LEADER
   ```
3. **Adjust GC & Heartbeat Limits**: If GC pauses are causing session expiration, increase ZooKeeper session timeout in Pinot controller configurations:
   ```properties
   controller.zk.session.timeout.ms=30000
   ```

---

## 7. Server Overload

### Symptom
Pinot Server becomes unresponsive. Connections are dropped, JVM threads spike, CPU utilization hits 100%, and query errors increase.

### Root Causes
* **Off-Heap Memory Exhaustion**: Pinot uses off-heap buffers for query execution. Too many concurrent, heavy aggregation queries saturate off-heap capacity.
* **Segment Accumulation**: Too many tiny segments loaded, saturating memory mappings (`mmap` limitations).

### Log Signatures
In `/var/log/messages` or container log:
```
kernel: Out of memory: Kill process 1245 (java) score 950 or sacrifice child
```

### Resolution Playbook
1. **Analyze Segment Count**: Check how many segments exist per table. If segments are small (under 100MB), configure **Pinot Minion** tasks to run **MergeRollup** to combine small segments into larger historical blocks.
2. **Increase max_map_count**: If Linux throws memory mapping limits, increase `max_map_count` on the host machine:
   ```bash
   sysctl -w vm.max_map_count=262144
   ```
3. **Apply Resource Limits**: Configure Pinot Broker to restrict maximum execution threads per query:
   ```properties
   pinot.broker.query.response.limit=10000
   ```
