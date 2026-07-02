# Production Troubleshooting Playbook: Kafka Producers & Consumers

This guide provides runbooks for operational issues that arise when running Kafka producers, consumers, and consumer groups in production.

---

## 🚨 Scenario 1: Consumer Lag Spiking
**Symptom**: Consumer lag increases on one or more partitions, causing delayed processing of downstream events.

### 🔍 Diagnostic Commands
Describe the consumer group and identify which partitions are lagging:
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-processing-group
```

Identify CPU, memory, or database write latencies on the lagging consumer nodes:
- Check JVM Garbage Collection logs (`jstat -gcutil <pid> 1000 10`).
- Thread dump analysis to locate blocking IO: `jcmd <pid> Thread.print`.

### 💡 Root Cause & Resolutions
*   **Root Cause 1: Sudden spike in traffic.**
    *   *Resolution*: Scale the consumer group by launching more consumer processes up to the number of partitions. (e.g., if topic has 6 partitions and you have 2 consumers, launch 4 more).
*   **Root Cause 2: Slow message processing (IO block, DB locking, external APIs).**
    *   *Resolution*: Optimize processing code. Use concurrent consumer worker pools within the process, or tune client fetch sizes (`max.poll.records`, `max.partition.fetch.bytes`) to process smaller batches.
*   **Root Cause 3: Under-partitioned topic.**
    *   *Resolution*: Increase partition count. Note that changing partition count affects key routing order. Run `kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic orders --partitions <new_count>`.

---

## 🚨 Scenario 2: Rebalance Storms
**Symptom**: Consumers are constantly dropping out of the group, and partitions are frequently being reassigned. Messages are processed repeatedly, and throughput drops to near-zero.

### 🔍 Diagnostic Commands
Check consumer group states and member details:
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-processing-group --state
```
Check application log messages for `CommitFailedException` or search for group coordinator log messages like `PreparingRebalance`.

### 💡 Root Cause & Resolutions
*   **Root Cause 1: Slow processing triggers `max.poll.interval.ms` timeout.**
    *   If a consumer takes longer than `max.poll.interval.ms` (default 300,000ms or 5 mins) to process records from a single poll, it intentionally leaves the group.
    *   *Resolution*: Decrease `max.poll.records` or increase `max.poll.interval.ms` in `consumer.properties`. Optimize batching logic to run asynchronously.
*   **Root Cause 2: GC pauses or network drops trigger `session.timeout.ms`.**
    *   The consumer fails to send heartbeats because JVM is blocked by a Full GC or network connection is lost.
    *   *Resolution*: Tune JVM GC flags (use G1GC or ZGC). Increase `session.timeout.ms` (e.g. to 45000ms) and ensure `heartbeat.interval.ms` is set to 1/3 of the value (15000ms).
*   **Root Cause 3: Eager assignment strategy causing complete cluster blockages.**
    *   *Resolution*: Upgrade consumer config to use the `CooperativeStickyAssignor` which supports incremental cooperative rebalancing, limiting downtime.

---

## 🚨 Scenario 3: CommitFailedException on Consumer
**Symptom**: Consumer throws `org.apache.kafka.clients.consumer.CommitFailedException` when calling `commitSync()`. Offsets are not saved, leading to duplicate consumption of the same records.

### 🔍 Diagnostic Commands
Look at application console logs to determine how long the consumer spent processing between poll loops.

### 💡 Root Cause & Resolutions
*   **Root Cause**: The time elapsed between two consecutive calls to `poll()` was longer than `max.poll.interval.ms`. The group coordinator has already kicked the consumer out and reassigned its partitions.
    *   *Resolution*:
        1. Reduce `max.poll.records` (e.g. from 500 to 50 or 10).
        2. Increase `max.poll.interval.ms` configuration.
        3. Offload heavy processing (e.g. image rendering, API calls) to an executor thread pool instead of blocking the main Kafka poll thread.

---

## 🚨 Scenario 4: Poison Pill (Deserialization Failures)
**Symptom**: A single malformed message causes the consumer to fail repeatedly, crash, and block progress on that partition completely.

### 🔍 Diagnostic Commands
Look at application stack traces for deserialization errors (e.g., `Jackson` json errors, `SerializationException`).

### 💡 Root Cause & Resolutions
*   **Root Cause**: A producer wrote a payload that does not match the schema or format expected by the consumer.
    *   *Resolution*:
        1. Implement an application-level try-catch block inside the poll loop. Log the bad message offset and key.
        2. Route the malformed raw bytes to a **Dead Letter Queue (DLQ)** topic (e.g., `orders-dlq`) for offline inspection.
        3. Manually skip the offset of the poison pill if needed using:
           ```bash
           kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group order-processing-group \
             --reset-offsets --to-offset <offset_to_skip_to> --topic orders:partition_id --execute
           ```
        4. Integrate Kafka Schema Registry (Avro, Protobuf) in subsequent phases to validate schemas before they are written.

---

## 🚨 Scenario 5: Partition Imbalance (Hot Partitions)
**Symptom**: One broker or consumer handles significantly more traffic than others, leading to localized memory saturation and high latency, while other nodes remain idle.

### 🔍 Diagnostic Commands
Check offset rates and byte throughput per partition:
```bash
kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic orders --time -1
```

### 💡 Root Cause & Resolutions
*   **Root Cause**: Poor partitioning key design. The key utilized in the producer (`customerId` or `tenantId`) has high cardinality skew (e.g., a single large customer generates 80% of all order events).
    *   *Resolution*:
        1. Introduce a compound key (e.g., `customerId_orderId`) to distribute payloads evenly.
        2. Use a custom partitioner implementation that handles skewed keys specially or falls back to round-robin.
        3. Do not specify a key if absolute message ordering per customer is not required (falling back to default sticky partition routing).

---

## 🚨 Scenario 6: Slow Producers (Throughput Bottleneck)
**Symptom**: The producer app is blocking on `send()` calls, or buffer pool memory is exhausted (`BufferExhaustedException`), causing the application to fail.

### 🔍 Diagnostic Commands
Check JMX metrics for:
- `buffer-available-bytes`
- `record-queue-time-max`
- `request-latency-max`

### 💡 Root Cause & Resolutions
*   **Root Cause 1: Broker network/disk bottlenecks.**
    *   The brokers are overwhelmed or cannot write to disk fast enough, slowing down acknowledgements.
    *   *Resolution*: Check broker logs for disk saturation. Add more brokers to split partition allocation.
*   **Root Cause 2: Inefficient batching settings.**
    *   The producer sends records individually because `linger.ms` is set to 0.
    *   *Resolution*: Increase `linger.ms` (e.g. to 20ms) and `batch.size` (e.g. to 64KB or 128KB). Enable compression (`zstd` or `lz4`) to reduce network payload size.
