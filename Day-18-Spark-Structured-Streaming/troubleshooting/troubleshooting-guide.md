# Day 18 — Spark Structured Streaming Troubleshooting Playbook
# Location: Day-18-Spark-Structured-Streaming/troubleshooting/troubleshooting-guide.md

This production playbook covers diagnosing and resolving operations, latency, memory, and consistency issues in Spark Structured Streaming jobs running over Apache Kafka and Hadoop storage.

---

## 🚨 1. Consumer Lag & Slow Micro-batches (Symptom: Processing Time > Trigger Interval)

### Symptoms
- Spark Streaming UI shows growing **Input Rate** while **Processing Rate** falls behind.
- Kafka Consumer Lag metrics (monitored via CLI or Prometheus) show continuously increasing offsets between broker log-end offsets and Spark read offsets.
- Batch processing times exceed the configured streaming trigger interval (e.g. processing a 10s batch takes 30s).

### Root Cause
- **Downstream Bottlenecks**: The sink (e.g. database, object storage) is slow or throttled.
- **Resource Under-provisioning**: CPU cores or memory are insufficient to process the volume of incoming records.
- **Data Skew**: Some partitions are receiving many more events than others, overloading specific executor cores.
- **Garbage Collection (GC) pauses**: Heavy GC pauses on executors interrupt the query pipeline.

### Logs to Inspect
Check executor logs (`stdout`/`stderr`) and query progress metrics:
- Look for messages showing long batch processing:
  ```text
  INFO MicroBatchExecution: Streaming query SparkStructuredStreamingLab [id = ..., runId = ...] progressed to batch 45
  ```
- Inspect query progress statistics printed or emitted:
  ```json
  "triggerExecution": {
    "addBatch": 24395,  // Duration in ms. High addBatch indicates sink write bottleneck.
    "getBatch": 150,
    "getOffset": 80,
    "walCommit": 450
  }
  ```

### Resolution
1. **Enable Backpressure**: Instruct Spark to dynamically adjust ingestion rates based on receiver throughput limits:
   ```properties
   spark.streaming.backpressure.enabled   true
   ```
2. **Limit Max Rate per Trigger**: Restrict the number of records read from Kafka per partition in a single micro-batch to prevent ingestion spikes:
   ```properties
   # Limit to 5000 records per partition per second
   spark.sql.streaming.kafka.maxOffsetsPerTrigger   50000
   ```
3. **Scale Kafka Partitions**: Align Spark executor core counts with Kafka partition sizes to optimize read parallelism. For example, if a topic has 12 partitions, allocate 12 executor cores.
4. **Tune Sink Batch Size**: For JDBC or write sinks, increase batch sizes and thread write pools.

---

## 🚨 2. State Store Out-Of-Memory (OOM) Errors (Symptom: Heap Exhaustion in Stateful Streaming)

### Symptoms
- Long-running streaming query fails with:
  ```text
  java.lang.OutOfMemoryError: Java heap space
  at org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStore...
  ```
- Executor nodes repeatedly crash and restart.

### Root Cause
- **Unbounded State Growth**: Stateful operations (like windowed aggregations or stream-stream joins) keep growing because watermarks are either missing, misconfigured, or events are arriving with keys that never trigger watermark cleanup.
- **HDFS-backed State Store Overhead**: The default state provider (`HDFSBackedStateStore`) stores all active state keys directly in the JVM Heap, which leads to high garbage collection pressure and OOMs at high key cardinalities.

### Logs to Inspect
Check executor diagnostics:
- Search for GC overhead limits exceeded:
  ```text
  WARN TaskSetManager: Lost task 2.0 in stage 14.0 (TID 118, executor 1): java.lang.OutOfMemoryError: GC overhead limit exceeded
  ```
- State store logs:
  ```text
  INFO HDFSBackedStateStoreProvider: Cleaned up 3 old files in state store
  ```

### Resolution
1. **Switch to RocksDB State Store Provider**: RocksDB stores data off-heap inside local executor files, avoiding heap OOMs:
   ```properties
   spark.sql.streaming.stateStore.providerClass   org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
   ```
2. **Enforce Watermarks**: Verify that stateful operations use `.withWatermark()` and that the column matches the event time. A missing watermark forces Spark to keep all keys in state forever.
3. **Increase Executor Off-Heap memory**: When using RocksDB, allocate off-heap memory for C++ allocations:
   ```properties
   spark.memory.offHeap.enabled   true
   spark.memory.offHeap.size      1g
   ```

---

## 🚨 3. Checkpoint Corruption or Incompatibility (Symptom: Job Fails to Resume)

### Symptoms
- Restarting a streaming job from an existing checkpoint fails with:
  ```text
  java.lang.IllegalStateException: Schema of the input stream has changed and is incompatible...
  ```
- Or errors pointing to serialization version mismatch:
  ```text
  java.io.IOException: Unexpected offset format version...
  ```

### Root Cause
- The application code was modified to change the query output schema, column types, or aggregation keys, violating checkpoint schema compatibility.
- An upgrade of Spark or the Kafka connector version occurred, and the internal offset/metadata formats are no longer backward compatible.

### Logs to Inspect
Look in the driver startup trace:
- Schema mismatch errors:
  ```text
  org.apache.spark.sql.execution.streaming.IncrementalExecution: Checkpoint file found, resuming query...
  AssertionError: assertion failed: Conflicting directory structures found in checkpoint location...
  ```

### Resolution
1. **Schema Changes require Checkpoint Reset**: If the logical query schema has modified column types or deleted columns, you MUST point to a new checkpoint location or delete the old checkpoint directory:
   ```bash
   hadoop fs -rm -r /tmp/spark-checkpoints/clickstream
   ```
2. **State Compatibility Settings**: If you need to make schema adjustments that are backward compatible, configure Spark to bypass strict schema checks:
   ```properties
   spark.sql.streaming.schemaEvolutionMode   true
   ```
3. **Isolate Checkpoint Paths per Query**: Ensure each query in your application has a unique sub-path in HDFS/S3, e.g., `/checkpoints/query1`, `/checkpoints/query2`. Never share checkpoint folders between queries.

---

## 🚨 4. Late-Arriving Data Dropped (Symptom: Empty Output or Missing Aggregated Records)

### Symptoms
- Data is visible in Kafka and ingest events are successful, but the output directories/sinks are empty.
- Aggregated metrics show no output rows for expected times.

### Root Cause
- **Skewed Watermarks**: The event time of some incoming events is far ahead of actual clock time (e.g. bad device clock showing year 2035). This advances the global watermark to that future date, causing all subsequent valid events (with current timestamps) to be discarded as "late data".
- **Append Mode Behavior**: In Append mode, aggregated window records are only written when the watermark passes the end of the window. If no new events arrive to advance the watermark, the window remains open and nothing is written.

### Logs to Inspect
Inspect query execution metrics JSON:
- Check `watermark` value and `droppedRecords` in progress JSON:
  ```json
  "sources" : [ {
    "description" : "KafkaV2[Subscribe[clickstream]]",
    "metrics" : {
      "numInputRows" : 1000,
      "watermark" : "2026-07-09T18:30:00.000Z" // If this is too far ahead, it drops normal data
    }
  } ],
  "stateOperators" : [ {
    "numRowsDroppedByWatermark" : 950  // Indicates data is being actively discarded
  } ]
  ```

### Resolution
1. **Validate Event Clock Senders**: Put a sanity check/filter in your code to drop records with anomalous future timestamps *before* watermarking:
   ```python
   # Filter out events with timestamps more than 5 minutes in the future
   clean_df = df.filter(col("event_time") <= current_timestamp() + expr("INTERVAL 5 MINUTES"))
   ```
2. **Configure Late Data Tolerance**: Increase the watermark duration (e.g. from `10 minutes` to `2 hours`) to accommodate standard network and producer delays.
3. **Change Output Mode for Testing**: If you need to see live updates without waiting for watermarks to expire, switch the output mode to `Update`. Note that this is only compatible with console, memory, or state-aware custom sinks (not standard Parquet/Delta files).

---

## 🚨 5. Duplicate Record Processing (Symptom: Multi-Write or At-Least-Once Violations)

### Symptoms
- Downstream database or storage contains duplicate records for the same transaction or window.
- Aggregate counts are higher than the actual number of events published.

### Root Cause
- **Sink Side At-Least-Once Limitation**: Spark guarantees *exactly-once* semantics internally by tracking offsets and commits in checkpoints. However, if an executor fails *during* a write operation and before writing the commit file, the retried batch will execute again. If the sink is not idempotent, duplicate records will appear.

### Logs to Inspect
Look in the HDFS checkpoint `commits` directory:
- If a batch ID (e.g., `12`) has an entry in `offsets` but no corresponding file in `commits`, Spark will reprocess batch 12 on restart.
- Look for executor recovery messages:
  ```text
  INFO MicroBatchExecution: Resuming at batch 12 with offsets ...
  ```

### Resolution
1. **Use Idempotent/Transactional Sinks**: Ensure your output target supports idempotent writes (e.g. UPSERTs in databases based on a unique key, or write transactional formats like Delta Lake or Apache Iceberg).
2. **File Sinks rely on Manifests**: When writing to Parquet/Orc, rely on Spark's transaction log (manifest files inside `_spark_metadata`). External query engines (like Presto or Trino) must read through the Spark catalog to avoid seeing duplicate uncommitted task files.
3. **Set Unique IDs**: For custom database sinks, use a composite key of `(window_start, window_end, group_key)` as a primary key to deduplicate duplicate batch retries.
