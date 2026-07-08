# Day 17 — Spark SQL & Catalyst Optimizer Troubleshooting Playbook
# Location: Day-17-Spark-SQL-Catalyst/troubleshooting/troubleshooting-guide.md

This production playbook covers diagnosing and resolving performance, compilation, and runtime issues in the Spark SQL engine, Catalyst optimizer, and Tungsten runtime.

---

## 🚨 1. Data Skew & Straggler Tasks (Symptom: 99% Tasks Done, 1 Task Hangs)

### Symptoms
- 99% of tasks in a join or aggregation stage complete in seconds, while 1 or 2 tasks run for 30 minutes.
- Thread dumps of stuck tasks show execution on `SortMergeJoin` or `HashAggregate`.

### Root Cause
- The join or aggregation key is unevenly distributed (e.g., millions of rows have key `NULL`, `""`, or a default ID like `0`). All rows with the same hash key are routed to a single executor partition thread.

### Logs to Inspect
Look in executor stdout/stderr or Spark History UI Task Metrics:
- Check **Max Task Duration** vs. **Median Task Duration**.
- Compare **Shuffle Read Size** across tasks; if the maximum task shuffle read is 5GB while the median is 2MB, data skew is present.

### Resolution
1. **Enable AQE Skew Join Optimization**:
   ```properties
   spark.sql.adaptive.skewJoin.enabled   true
   spark.sql.adaptive.skewJoin.skewedPartitionFactor   5
   spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes   268435456  # 256MB
   ```
2. **Filter Out Skewed/Null Keys**: If the skew key is not needed in the result (e.g. null joins), filter it out before the join.
3. **Key Salting**: For valid join keys that are heavily skewed, append a random number suffix (e.g. 1 to N) to the key on the left side, and replicate the corresponding keys 1 to N on the right side to distribute the join keys across N executors.

---

## 🚨 2. Broadcast Join Out-Of-Memory (OOM) Errors (Symptom: OutOfMemoryError in Driver or Executor)

### Symptoms
- Spark job fails during broadcating with:
  ```text
  java.lang.OutOfMemoryError: Java heap space
  at org.apache.spark.sql.execution.joins.LongHashedRelation.asReadOnlyCopy
  ```
- Or Driver process crashes with an OOM.

### Root Cause
- The broadcast threshold `spark.sql.autoBroadcastJoinThreshold` is configured too high, or the catalog stats estimated a table is small (e.g., 5MB) but it actually contains 500MB of compressed data, which expands to several gigabytes of uncompressed Java objects.

### Logs to Inspect
Check Driver logs for:
- `Could not execute broadcast relation...`
- `OutOfMemoryError: Java heap space` on Driver during `BroadcastExchangeExec`.

### Resolution
1. **Lower or Disable Broadcast Threshold**:
   Disable broadcasting for the problematic queries:
   ```properties
   spark.sql.autoBroadcastJoinThreshold   -1
   ```
2. **Increase Driver Memory**:
   ```properties
   spark.driver.memory   4g
   ```
3. **Run ANALYZE TABLE**: Update Metastore stats so Catalyst knows the true sizes of tables:
   ```sql
   ANALYZE TABLE table_name COMPUTE STATISTICS FOR COLUMNS;
   ```

---

## 🚨 3. AQE Not Triggering (Symptom: Static Plan executed without runtime adaptivity)

### Symptoms
- Spark History SQL UI does not show `AdaptiveSparkPlan` in the execution block.
- Partitions are not coalesced, and join strategies do not change.

### Root Cause
- AQE requires at least one shuffle boundary (e.g. GROUP BY, JOIN, DISTINCT) to trigger runtime statistics gathering.
- AQE is globally disabled, or conflicting configs (like manual partition distribution) prevent adaptivity.

### Logs to Inspect
Check logs for config options:
- `spark.sql.adaptive.enabled` output.
- Log message: `Adaptive query execution is enabled...`

### Resolution
1. **Enable AQE explicitly**:
   ```properties
   spark.sql.adaptive.enabled   true
   ```
2. **Verify Shuffle Boundaries**: Ensure the query has operation steps that force shuffles. If it's a simple `SELECT * FROM table WHERE filter`, no shuffle occurs, and AQE will not construct adaptive stages.

---

## 🚨 4. Predicate Pushdown Not Applied (Symptom: Massive Data Read from Storage)

### Symptoms
- Storage network bandwidth is exhausted.
- Filters are executed inside the Spark JVM rather than inside the storage layer (e.g., Parquet file reader or external database).

### Root Cause
- The query uses functions in the filter (e.g., `WHERE LOWER(name) = 'alice'`), preventing Spark from pushing the filter down.
- The storage format does not support pushdown, or the configuration is disabled.

### Logs to Inspect
Inspect the physical plan output from `.explain(true)`:
- Look at the `FileScan` node. Check the `PushedFilters` attribute.
- If `PushedFilters: []` or does not contain your criteria, pushdown failed.

### Resolution
1. **Avoid functions on columns in filter clauses**: Instead of `WHERE date_add(col, 2) > '2026-01-01'`, write `WHERE col > date_sub('2026-01-01', 2)`.
2. **Ensure Pushdown Settings are Enabled**:
   ```properties
   spark.sql.parquet.filterPushdown   true
   spark.sql.orc.filterPushdown       true
   ```

---

## 🚨 5. Whole-Stage Codegen Failures (Symptom: Codegen fallback to Volcano Iterator)

### Symptoms
- Warning logs indicating that Java compilation failed.
- Execution slowdown because Spark is falling back to slow Volcano-style row-by-row iteration.

### Root Cause
- The generated Java method size exceeds the 64KB JVM byte-code limit. This occurs when queries contain hundreds of columns or complex nested expressions.

### Logs to Inspect
Search application logs for:
- `WARN WholeStageCodegenExec: Failed to compile: org.codehaus.commons.compiler.CompileException...`
- `WARN WholeStageCodegenExec: Method too large`

### Resolution
1. **Split Large Queries**: Break up giant queries with hundreds of SELECT columns or CASE-WHEN statements into intermediate tables or stages.
2. **Tune Codegen Limit Settings**: If necessary, adjust max fields threshold:
   ```properties
   spark.sql.codegen.maxFields   100
   ```

---

## 🚨 6. Incorrect Join Strategies (Symptom: ShuffleHashJoin used instead of Broadcast)

### Symptoms
- Massive networks shuffles when joining a large dataset with a tiny config dataset.

### Root Cause
- Spark does not know the catalog statistics of the small dataset, or the threshold is set too low.

### Logs to Inspect
Check the physical plan for `SortMergeJoin` and accompanying `Exchange hashpartitioning`.

### Resolution
1. **Force Join Hint**:
   Use SQL hints to force a broadcast:
   ```sql
   SELECT /*+ BROADCAST(c) */ u.id, c.city_name FROM users u JOIN cities c ON u.city_id = c.city_id
   ```
2. **Update Spark Session thresholds**:
   ```properties
   spark.sql.autoBroadcastJoinThreshold   20971520  # 20MB
   ```
